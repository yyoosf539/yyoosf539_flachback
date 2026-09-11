

use alloy::network::{EthereumWallet, TransactionBuilder, NetworkTransactionBuilder, TransactionResponse};
use alloy::consensus::Transaction as ConsensusTransaction;
use alloy::primitives::{address, Address, U256, Bytes, B256};
use alloy::providers::{Provider, ProviderBuilder, WsConnect, IpcConnect};
use alloy::rpc::types::{TransactionRequest, BlockNumberOrTag, Filter, Log, Transaction};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use alloy::sol_types::{SolCall, SolEvent};
use alloy::eips::eip2718::Encodable2718;
use futures_util::stream::StreamExt;
use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{watch, RwLock, Semaphore};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use reqwest::Client;
use tracing::{info, warn, error, debug};

use axum::{routing::get, Json, Router, extract::State, response::Html};
use serde::{Serialize, Deserialize};
use std::net::SocketAddr;

use revm::database::{AlloyDB, CacheDB, WrapDatabaseAsync};
use revm::state::{AccountInfo, Bytecode};
use revm::primitives::{TxKind, Bytes as RevmBytes};
use revm::context::TxEnv;
use revm::{Context, MainBuilder, MainContext, ExecuteEvm, ExecuteCommitEvm};

sol! {
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }

    interface IMulticall3 {
        struct Call3 {
            address target;
            bool allowFailure;
            bytes callData;
        }
        struct Result {
            bool success;
            bytes returnData;
        }
        function aggregate3(Call3[] calldata calls) external payable returns (Result[] memory returnData);
    }

    interface IUniswapV2Pair {
        function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast);
        function token0() external view returns (address);
        function token1() external view returns (address);
    }

    interface IUniswapV2Factory {
        function getPair(address tokenA, address tokenB) external view returns (address pair);
    }
}

const MULTICALL3: Address = address!("cA11bde05977b3631167028862bE2a173976CA11");

sol! {
    interface ISuperSecureArbitrage {
        struct ArbParams {
            address pair1;
            address pair2;
            address factory1;
            address factory2;
            address borrowToken;
            uint256 borrowAmount;
            uint256 amount0OutPair2;
            uint256 amount1OutPair2;
            address repayToken;
            uint256 repayAmount;
            address profitToken;
            uint256 minProfit;
            uint256 deadline;
        }

        function executeFlashArbitrage(
            ArbParams calldata params,
            uint256 amount0OutPair1,
            uint256 amount1OutPair1
        ) external;
    }

    event Sync(uint112 reserve0, uint112 reserve1);

    interface IUniswapV2Router {
        function swapExactTokensForTokens(
            uint256 amountIn,
            uint256 amountOutMin,
            address[] calldata path,
            address to,
            uint256 deadline
        ) external returns (uint256[] memory amounts);

        function swapExactETHForTokens(
            uint256 amountOutMin,
            address[] calldata path,
            address to,
            uint256 deadline
        ) external payable returns (uint256[] memory amounts);

        function swapExactTokensForETH(
            uint256 amountIn,
            uint256 amountOutMin,
            address[] calldata path,
            address to,
            uint256 deadline
        ) external returns (uint256[] memory amounts);

        function swapExactTokensForTokensSupportingFeeOnTransferTokens(
            uint256 amountIn,
            uint256 amountOutMin,
            address[] calldata path,
            address to,
            uint256 deadline
        ) external;
    }
}

#[derive(Clone, Copy, Default)]
struct GasState {
    base_fee: U256,
    current_block: u64,
}

#[derive(Clone, Copy)]
struct SimulationResult {
    is_profitable: bool,
    expected_profit: U256,
    gas_used: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct PoolState {
    reserve0: U256,
    reserve1: U256,
    fee_bps: u32, 
    token0: Address,
    token1: Address,
}

type MarketState = Arc<RwLock<HashMap<Address, PoolState>>>;

type CodeCache = Arc<RwLock<HashMap<Address, Bytes>>>;

type PairRegistry = HashMap<(Address, Address), Vec<(Address, String)>>;

type PairDexMap = HashMap<Address, String>;

type RouterDexMap = HashMap<Address, String>;

type DexFactoryMap = HashMap<String, Address>;

fn sorted_pair(a: Address, b: Address) -> (Address, Address) {
    if a < b { (a, b) } else { (b, a) }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum TradeStatus {
    Submitted,
    Won,
    Lost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LossReason {
    NotIncluded,
    Reverted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TradeRecord {
    tx_hash: String,
    victim_tx_hash: String,
    timestamp: u64,
    expected_profit_wei: String,
    /// تكلفة الغاز المتوقعة وقت الإرسال (base_fee فقط، تقريبية)
    expected_gas_cost_wei: String,
    /// الغاز الفعلي المصروف (يُملأ بعد ما تنضم المعاملة فعليًا بالبلوك)
    actual_gas_used: Option<u64>,
    /// التكلفة الفعلية بالـ wei (gas_used * effective_gas_price من الـ receipt)
    actual_gas_cost_wei: Option<String>,
    /// صافي الربح الفعلي (يبقى None لحد ما نعرف النتيجة)
    actual_net_profit_wei: Option<String>,
    status: TradeStatus,
    /// سبب الخسارة (None إذا Won أو Submitted)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    loss_reason: Option<LossReason>,
    /// DEX الضحية / pair1
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dex1: Option<String>,
    /// DEX المراجحة / pair2
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dex2: Option<String>,
    /// عدد builders اللي أرسلنا لهم
    #[serde(default, skip_serializing_if = "Option::is_none")]
    builder_count: Option<u32>,
}

/// سجل الصفقات بالذاكرة، مفتاحه هاش معاملتنا (بحث/تحديث O(1))
type TradeLog = Arc<RwLock<HashMap<B256, TradeRecord>>>;

const TRADES_FILE: &str = "trades.json";

/// تحميل الصفقات المحفوظة من تشغيل سابق (إن وُجدت) حتى تضل اللوحة فيها تاريخ كامل
/// حتى لو انعاد تشغيل البوت.
async fn load_trade_log() -> TradeLog {
    let map: HashMap<B256, TradeRecord> = match tokio::fs::read(TRADES_FILE).await {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
        Err(_) => HashMap::new(),
    };
    info!("📊 processing {} sequence shares {TRADES_FILE}", map.len());
    Arc::new(RwLock::new(map))
}

/// حفظ كامل سجل الصفقات على القرص (كتابة الملف كامل — حجم السجل صغير عمليًا
/// نسبة لعدد صفقات MEV اليومي الواقعي، فما في داعي لتحسين append هون).
async fn persist_trade_log(log: &TradeLog) {
    let snapshot = log.read().await.clone();
    match serde_json::to_vec(&snapshot) {
        Ok(bytes) => {
            if let Err(e) = tokio::fs::write(TRADES_FILE, bytes).await {
                warn!("فشل حفظ {TRADES_FILE}: {e}");
            }
        }
        Err(e) => warn!("swap sequence failed {TRADES_FILE}: {e}"),
    }
}

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// يسجّل صفقة جديدة فور إرسال الـ bundle (حالتها Submitted بالربح/التكلفة المتوقعين من REVM)
async fn record_submitted_trade(
    log: &TradeLog,
    tx_hash: B256,
    victim_tx_hash: B256,
    expected_profit: U256,
    expected_gas_cost: U256,
    dex1: &str,
    dex2: &str,
    builder_count: u32,
) {
    let record = TradeRecord {
        tx_hash: tx_hash.to_string(),
        victim_tx_hash: victim_tx_hash.to_string(),
        timestamp: now_unix(),
        expected_profit_wei: expected_profit.to_string(),
        expected_gas_cost_wei: expected_gas_cost.to_string(),
        actual_gas_used: None,
        actual_gas_cost_wei: None,
        actual_net_profit_wei: None,
        status: TradeStatus::Submitted,
        loss_reason: None,
        dex1: Some(dex1.to_string()),
        dex2: Some(dex2.to_string()),
        builder_count: Some(builder_count),
    };
    log.write().await.insert(tx_hash, record);
    persist_trade_log(log).await;
}


async fn watch_trade_outcome<P: Provider>(
    provider: Arc<P>,
    log: TradeLog,
    tx_hash: B256,
    gas_limit_used_estimate: u64,
) {
    const MAX_BLOCKS_TO_WAIT: u32 = 5;
    const POLL_INTERVAL: Duration = Duration::from_millis(600);

    let mut attempts = 0u32;
    // تقدير عدد محاولات polling يقارب MAX_BLOCKS_TO_WAIT (بولygon ~2s/block تقريبًا)
    let max_attempts = (MAX_BLOCKS_TO_WAIT * 4).max(4);

    loop {
        attempts += 1;
        match provider.get_transaction_receipt(tx_hash).await {
            Ok(Some(receipt)) => {
                let success = receipt.status();
                let gas_used = receipt.gas_used as u64;
                let effective_gas_price = U256::from(receipt.effective_gas_price);
                let actual_gas_cost = U256::from(gas_used).saturating_mul(effective_gas_price);

                let mut map = log.write().await;
                if let Some(rec) = map.get_mut(&tx_hash) {
                    rec.actual_gas_used = Some(gas_used);
                    rec.actual_gas_cost_wei = Some(actual_gas_cost.to_string());
                    if success {
                        let expected_profit: U256 =
                            rec.expected_profit_wei.parse().unwrap_or(U256::ZERO);
                        rec.actual_net_profit_wei =
                            Some(expected_profit.saturating_sub(actual_gas_cost).to_string());
                        rec.status = TradeStatus::Won;
                        rec.loss_reason = None;
                        info!("✅ arbitrage successful {tx_hash}: gas actual ={gas_used}");
                    } else {
                        rec.actual_net_profit_wei = Some(format!("-{}", actual_gas_cost));
                        rec.status = TradeStatus::Lost;
                        rec.loss_reason = Some(LossReason::Reverted);
                        warn!("❌ transaction included but reverted :(revert) {tx_hash}");
                    }
                }
                drop(map);
                persist_trade_log(&log).await;
                return;
            }
            Ok(None) => { /* لسا ما انضمّت، كمل بالانتظار */ }
            Err(e) => {
                warn!("failed to fetch receipt for transaction {tx_hash}: {e}");
            }
        }

        if attempts >= max_attempts {
            // ما انضمّت خلال النافذة المسموحة — نعتبرها فرصة فائتة (بوت آخر سبقنا على
            // الأغلب، أو الـ builder ما ضمّنها بالبلوك). خسارة موثّقة بدل تجاهل صامت.
            let mut map = log.write().await;
            if let Some(rec) = map.get_mut(&tx_hash) {
                if rec.status == TradeStatus::Submitted {
                    rec.status = TradeStatus::Lost;
                    rec.loss_reason = Some(LossReason::NotIncluded);
                    rec.actual_gas_used = Some(0);
                    rec.actual_gas_cost_wei = Some("0".to_string());
                    rec.actual_net_profit_wei = Some("0".to_string());
                    let _ = gas_limit_used_estimate;
                    warn!("⌛ {tx_hash} not included within {MAX_BLOCKS_TO_WAIT} blocks — recorded as loss (frontrun or builder rejected)");
                }
            }
            drop(map);
            persist_trade_log(&log).await;
            return;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[derive(Serialize)]
struct DashboardStats {
    total_trades: usize,
    won: usize,
    lost: usize,
    pending: usize,
    win_rate_pct: f64,
    total_gas_spent_wei: String,
    total_net_profit_wei: String,
    lost_not_included: usize,
    lost_reverted: usize,
    pairs_monitored: usize,
    dex_count: usize,
}

async fn api_stats(State(state): State<AppState>) -> Json<DashboardStats> {
    let map = state.trade_log.read().await;
    let mut won = 0usize;
    let mut lost = 0usize;
    let mut pending = 0usize;
    let mut lost_not_included = 0usize;
    let mut lost_reverted = 0usize;
    let mut total_gas_spent = U256::ZERO;
    let mut total_net_profit = U256::ZERO;

    for rec in map.values() {
        match rec.status {
            TradeStatus::Won => won += 1,
            TradeStatus::Lost => {
                lost += 1;
                match rec.loss_reason {
                    Some(LossReason::NotIncluded) => lost_not_included += 1,
                    Some(LossReason::Reverted) => lost_reverted += 1,
                    None => lost_not_included += 1,
                }
            }
            TradeStatus::Submitted => pending += 1,
        }
        if let Some(gas_str) = &rec.actual_gas_cost_wei {
            total_gas_spent = total_gas_spent.saturating_add(gas_str.parse().unwrap_or(U256::ZERO));
        }
        if rec.status == TradeStatus::Won {
            if let Some(p) = &rec.actual_net_profit_wei {
                total_net_profit = total_net_profit.saturating_add(p.parse().unwrap_or(U256::ZERO));
            }
        }
    }

    let total = map.len();
    let decided = won + lost;
    let win_rate_pct = if decided > 0 { (won as f64 / decided as f64) * 100.0 } else { 0.0 };

    Json(DashboardStats {
        total_trades: total,
        won,
        lost,
        pending,
        win_rate_pct,
        total_gas_spent_wei: total_gas_spent.to_string(),
        total_net_profit_wei: total_net_profit.to_string(),
        lost_not_included,
        lost_reverted,
        pairs_monitored: state.meta.pairs_monitored,
        dex_count: state.meta.dex_count,
    })
}

async fn api_trades(State(state): State<AppState>) -> Json<Vec<TradeRecord>> {
    let map = state.trade_log.read().await;
    let mut trades: Vec<TradeRecord> = map.values().cloned().collect();
    trades.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    trades.truncate(500); // آخر 500 صفقة كافية لعرض اللوحة، والملف على القرص فيه التاريخ كامل
    Json(trades)
}

async fn dashboard_index() -> Html<&'static str> {
    Html(include_str!("dashboard.html"))
}

fn build_dashboard_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(dashboard_index))
        .route("/api/stats", get(api_stats))
        .route("/api/trades", get(api_trades))
        .with_state(state)
}

async fn run_dashboard_server(state: AppState, port: u16) {
    let app = build_dashboard_router(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("📊 trade monitor running at http://{addr}");
    match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => {
            if let Err(e) = axum::serve(listener, app).await {
                error!("dashbord server stoped : {e}");
            }
        }
        Err(e) => error!("failed to bind dashboard port {port}: {e}"),
    }
}

fn parse_known_pairs(raw: &str) -> (HashSet<Address>, PairRegistry, PairDexMap) {
    let mut flat = HashSet::new();
    let mut registry: PairRegistry = HashMap::new();
    let mut pair_dex: PairDexMap = HashMap::new();

    for entry in raw.split(',').filter(|s| !s.is_empty()) {
        let parts: Vec<&str> = entry.split(':').collect();
        if parts.len() != 4 {
            warn!("input ignored KNOWN_PAIRS invalid (property pair:token0:token1:dexId): {entry}");
            continue;
        }
        let (Ok(pair), Ok(t0), Ok(t1)) = (
            parts[0].parse::<Address>(),
            parts[1].parse::<Address>(),
            parts[2].parse::<Address>(),
        ) else {
            warn!("failed to parse addresses KNOWN_PAIRS: {entry}");
            continue;
        };
        let dex_id = parts[3].to_string();
        if dex_id.is_empty() {
            warn!("ignored input KNOWN_PAIRS without dexId: {entry}");
            continue;
        }
        flat.insert(pair);
        pair_dex.insert(pair, dex_id.clone());
        registry.entry(sorted_pair(t0, t1)).or_default().push((pair, dex_id));
    }
    (flat, registry, pair_dex)
}

fn parse_known_routers(raw: &str) -> RouterDexMap {
    let mut map = RouterDexMap::new();
    for entry in raw.split(',').filter(|s| !s.is_empty()) {
        let parts: Vec<&str> = entry.split(':').collect();
        if parts.len() != 2 {
            warn!("ignored input KNOWN_ROUTERS invalid (expected router:dexId): {entry}");
            continue;
        }
        let Ok(addr) = parts[0].parse::<Address>() else {
            warn!("failed to parse address KNOWN_ROUTERS: {entry}");
            continue;
        };
        if parts[1].is_empty() {
            warn!("ignored input KNOWN_ROUTER without dexId: {entry}");
            continue;
        }
        map.insert(addr, parts[1].to_string());
    }
    map
}

fn parse_dex_factories(raw: &str) -> DexFactoryMap {
    let mut map = DexFactoryMap::new();
    for entry in raw.split(',').filter(|s| !s.is_empty()) {
        let parts: Vec<&str> = entry.split(':').collect();
        if parts.len() != 2 {
            warn!("input ignored DEX_FACTORIES invalid (expected dexId:factoryAddr): {entry}");
            continue;
        }
        let Ok(addr) = parts[1].parse::<Address>() else {
            warn!("failed to parse address DEX_FACTORIES: {entry}");
            continue;
        };
        if parts[0].is_empty() {
            warn!("ignored input DEX_FACTORIES without dexId: {entry}");
            continue;
        }
        map.insert(parts[0].to_string(), addr);
    }
    map
}

const DEFAULT_BUILDERS: &[&str] = &[
    "https://polygon.api.blxrbdn.com",
    "https://rpc.fastlane.finance",
];

/// Polygon defaults — تُستخدم إذا env فارغ (QuickSwap + Sushi + ApeSwap)
const DEFAULT_ROUTERS_RAW: &str = "\
0xa5E0829CaCEd8fFDD4De3c43696c57F7D7A678ff:quickswap,\
0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506:sushiswap,\
0xC0788A3aD43d79aa53B09c2EaCc313A787d1d607:apeswap";

const DEFAULT_FACTORIES_RAW: &str = "\
quickswap:0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32,\
sushiswap:0xc35DADB65012eC5796536bD9864eD8773aBc74C4,\
apeswap:0xCf083Be4164828f00cAE704EC15a36D711491284";

/// توكنات Polygon السائلة — C(6,2)=15 زوج × 3 DEX = 45 pool تُكتشف تلقائيًا
const DEFAULT_TOKEN_LIST_RAW: &str = "\
0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270,\
0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174,\
0x7ceb23fd6bc0Add59E62ac25578270cff1b9f619,\
0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6,\
0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063,\
0xc2132D05D31c914a87C6611C10748AEb04B58e8F";

const ZERO_ADDRESS: Address = address!("0000000000000000000000000000000000000000");
const WMATIC: Address = address!("0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270");
/// نسبة رشوة المعدّن من صافي الربح، بمقياس basis points قياسي (10,000 = 100%).
/// 900 = 9%، 9000 = 90%. قابلة للضبط عبر متغير البيئة BRIBE_BPS.
const DEFAULT_BRIBE_BPS: u64 = 9000;
/// الحد الأقصى المسموح لـ BRIBE_BPS (سقف أمان). قابل للضبط عبر متغير البيئة MAX_BRIBE_BPS.
/// مهما كانت القيمة، بيضل مقيّد دفاعيًا تحت 9999 (لا رشوة ≥100% من الربح).
const DEFAULT_MAX_BRIBE_BPS: u64 = 9000;

/// عدد المهام المتزامنة للمحاكاة (Semaphore)
const DEFAULT_SIM_CONCURRENCY: usize = 128;

/// نافذة البلوكات للـ bundle: target = current+1، max = current+BUNDLE_BLOCK_WINDOW
const DEFAULT_BUNDLE_BLOCK_WINDOW: u64 = 3;

#[derive(Clone)]
struct BotMeta {
    pairs_monitored: usize,
    dex_count: usize,
}

#[derive(Clone)]
struct AppState {
    trade_log: TradeLog,
    meta: BotMeta,
}

fn env_flag(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_token_list(raw: &str) -> Vec<Address> {
    raw.split(',')
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.trim().parse::<Address>().ok())
        .collect()
}

/// يدمج registry يدوي + مكتشف (بدون تكرار pair+dex)
fn merge_pair_registry(
    flat: &mut HashSet<Address>,
    registry: &mut PairRegistry,
    pair_dex: &mut PairDexMap,
    extra_flat: HashSet<Address>,
    extra_registry: PairRegistry,
    extra_pair_dex: PairDexMap,
) {
    flat.extend(extra_flat);
    pair_dex.extend(extra_pair_dex);
    for (tokens, entries) in extra_registry {
        registry.entry(tokens).or_default().extend(entries);
    }
}

/// يكتشف كل أزواج V2 الموجودة عبر factory.getPair لكل تركيبة توكنين × DEX (Multicall واحد)
async fn discover_pairs_multicall<P: Provider>(
    provider: &Arc<P>,
    tokens: &[Address],
    dex_factories: &DexFactoryMap,
) -> (HashSet<Address>, PairRegistry, PairDexMap) {
    let mut flat = HashSet::new();
    let mut registry: PairRegistry = HashMap::new();
    let mut pair_dex: PairDexMap = HashMap::new();

    if tokens.len() < 2 || dex_factories.is_empty() {
        return (flat, registry, pair_dex);
    }

    let mut calls: Vec<IMulticall3::Call3> = Vec::new();
    let mut meta: Vec<(Address, Address, String)> = Vec::new();

    for (dex_id, factory) in dex_factories {
        for i in 0..tokens.len() {
            for j in (i + 1)..tokens.len() {
                let (t0, t1) = sorted_pair(tokens[i], tokens[j]);
                calls.push(IMulticall3::Call3 {
                    target: *factory,
                    allowFailure: true,
                    callData: IUniswapV2Factory::getPairCall {
                        tokenA: t0,
                        tokenB: t1,
                    }
                    .abi_encode()
                    .into(),
                });
                meta.push((t0, t1, dex_id.clone()));
            }
        }
    }

    if calls.is_empty() {
        return (flat, registry, pair_dex);
    }

    let tx = TransactionRequest::default()
        .with_to(MULTICALL3)
        .with_input(IMulticall3::aggregate3Call { calls }.abi_encode());

    let Ok(bytes) = provider.call(tx).await else {
        warn!("pair auto-discovery failed (Multicall3)");
        return (flat, registry, pair_dex);
    };
    let Ok(decoded) = IMulticall3::aggregate3Call::abi_decode_returns(&bytes) else {
        warn!("failed to decode pair discovery Multicall3 response");
        return (flat, registry, pair_dex);
    };

    for (res, (t0, t1, dex_id)) in decoded.iter().zip(meta.iter()) {
        if !res.success {
            continue;
        }
        let Ok(pair_addr) = IUniswapV2Factory::getPairCall::abi_decode_returns(&res.returnData) else {
            continue;
        };
        if pair_addr == ZERO_ADDRESS {
            continue;
        }
        flat.insert(pair_addr);
        pair_dex.insert(pair_addr, dex_id.clone());
        registry
            .entry(sorted_pair(*t0, *t1))
            .or_default()
            .push((pair_addr, dex_id.clone()));
    }

    info!(
        "🔍 auto-discovery: {} pools across {} token pairs × {} DEX(s)",
        flat.len(),
        tokens.len() * (tokens.len().saturating_sub(1)) / 2,
        dex_factories.len()
    );
    (flat, registry, pair_dex)
}

/// تقدير غاز ثابت للفلترة السريعة قبل REVM (أخف من محاكاة كاملة).
const FAST_FILTER_GAS_ESTIMATE: u64 = 350_000;

fn parse_builder_urls() -> Vec<String> {
    env::var("BUILDER_URLS")
        .ok()
        .filter(|s| !s.is_empty())
        .map(|raw| raw.split(',').map(|u| u.trim().to_string()).filter(|u| !u.is_empty()).collect())
        .unwrap_or_else(|| DEFAULT_BUILDERS.iter().map(|s| s.to_string()).collect())
}

/// يتصل بالنود: IPC محلي أولاً (أقل latency)، وإلا WebSocket.
async fn connect_provider(
    wallet: EthereumWallet,
) -> Result<impl Provider + Clone, Box<dyn std::error::Error>> {
    if let Ok(ipc_path) = env::var("IPC_PATH") {
        info!("⚡ connecting via IPC: {ipc_path}");
        return Ok(ProviderBuilder::new()
            .wallet(wallet)
            .connect_ipc(IpcConnect::new(ipc_path))
            .await?);
    }
    let ws_rpc_url = env::var("WS_RPC_URL")
        .expect("IPC_PATH (مفضل) أو WS_RPC_URL مطلوب");
    info!("⏳ connecting via WebSocket: {ws_rpc_url}");
    Ok(ProviderBuilder::new()
        .wallet(wallet)
        .connect_ws(WsConnect::new(ws_rpc_url))
        .await?)
}

sol! {
    interface IOwnable {
        function owner() external view returns (address);
    }
}

/// العقد فيه onlyOwner على executeFlashArbitrage — لازم محفظة البوت = owner.
async fn verify_contract_owner<P: Provider>(
    provider: &P,
    contract: Address,
    bot: Address,
) -> Result<(), Box<dyn std::error::Error>> {
    let code = provider.get_code_at(contract).await?;
    if code.is_empty() {
        warn!(
            "⚠️ no bytecode found at {contract} — contract not yet deployed");
        return Ok(());
    }
    let call = IOwnable::ownerCall {};
    let tx = TransactionRequest::default()
        .with_to(contract)
        .with_input(call.abi_encode());
    let bytes = provider.call(tx).await?;
    let owner = IOwnable::ownerCall::abi_decode_returns(&bytes)?;
    if owner != bot {
        return Err(format!(
            "محفظة البوت ({bot}) ليست owner للعقد ({contract}) — owner الحالي: {owner}. \
             نفّذ: transferOwnership({bot})"
        )
        .into());
    }
    info!("✅ wallet bot= owner contract — executeFlashArbitrage allowed");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

    let private_key = env::var("PRIVATE_KEY").expect("PRIVATE_KEY required");

    let signer: PrivateKeySigner = private_key.parse()?;
    let wallet = EthereumWallet::from(signer.clone());
    let my_address = signer.address();

    let my_contract: Address = env::var("CONTRACT_ADDRESS")
        .expect("CONTRACT_ADDRESS required")
        .parse()?;

    let routers_raw = env::var("KNOWN_ROUTERS").unwrap_or_else(|_| DEFAULT_ROUTERS_RAW.to_string());
    let factories_raw = env::var("DEX_FACTORIES").unwrap_or_else(|_| DEFAULT_FACTORIES_RAW.to_string());
    let auto_discover = env_flag("AUTO_DISCOVER_PAIRS", true);

    let router_dex: RouterDexMap = parse_known_routers(&routers_raw);
    let dex_factories: DexFactoryMap = parse_dex_factories(&factories_raw);
    let dex_count = dex_factories.len();

    let (mut known_pairs_set, mut pair_registry, mut pair_dex_map) =
        parse_known_pairs(&env::var("KNOWN_PAIRS").unwrap_or_default());

    info!("🔌 Connecting to network...");
    let provider = Arc::new(connect_provider(wallet.clone()).await?);
    verify_contract_owner(provider.as_ref(), my_contract, my_address).await?;

    if auto_discover {
        let tokens_raw = env::var("TOKEN_LIST").unwrap_or_else(|_| DEFAULT_TOKEN_LIST_RAW.to_string());
        let tokens = parse_token_list(&tokens_raw);
        let (disc_flat, disc_reg, disc_pd) =
            discover_pairs_multicall(&provider, &tokens, &dex_factories).await;
        merge_pair_registry(
            &mut known_pairs_set,
            &mut pair_registry,
            &mut pair_dex_map,
            disc_flat,
            disc_reg,
            disc_pd,
        );
    }

    let pairs_monitored = known_pairs_set.len();
    info!(
        "📡 monitoring {} pools, {} routers, {} DEX(s)",
        pairs_monitored,
        router_dex.len(),
        dex_count
    );

    let known_routers: Arc<HashSet<Address>> = Arc::new(router_dex.keys().copied().collect());
    let router_dex: Arc<RouterDexMap> = Arc::new(router_dex);
    let known_pairs: Arc<HashSet<Address>> = Arc::new(known_pairs_set);
    let pair_registry = Arc::new(pair_registry);
    let pair_dex: Arc<PairDexMap> = Arc::new(pair_dex_map);
    let dex_factories: Arc<DexFactoryMap> = Arc::new(dex_factories);

    let max_bribe_bps = env_u64("MAX_BRIBE_BPS", DEFAULT_MAX_BRIBE_BPS).min(9999); // سقف دفاعي: أبدًا لا يوصل 100%
    let bribe_bps_configured = env_u64("BRIBE_BPS", DEFAULT_BRIBE_BPS);
    let bribe_bps = bribe_bps_configured.min(max_bribe_bps);
    if bribe_bps_configured > max_bribe_bps {
        warn!("BRIBE_BPS={bribe_bps_configured} من الـ.env تجاوز MAX_BRIBE_BPS={max_bribe_bps}، تم تقييده فعليًا إلى {bribe_bps}");
    }
    info!("bribe_bps الفعلي المستخدم = {bribe_bps} ({:.1}%) | الحد الأقصى المسموح به = {max_bribe_bps}", bribe_bps as f64 / 100.0);
    let bundle_block_window = env_u64("BUNDLE_BLOCK_WINDOW", DEFAULT_BUNDLE_BLOCK_WINDOW).max(1);
    let sim_concurrency = env_usize("SIM_CONCURRENCY", DEFAULT_SIM_CONCURRENCY);

    let trade_log: TradeLog = load_trade_log().await;
    let dashboard_port: u16 = env::var("DASHBOARD_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(4000);
    {
        let app_state = AppState {
            trade_log: trade_log.clone(),
            meta: BotMeta {
                pairs_monitored,
                dex_count,
            },
        };
        tokio::spawn(async move {
            run_dashboard_server(app_state, dashboard_port).await;
        });
    }

    let builder_urls = Arc::new(parse_builder_urls());
    let bloxroute_auth = env::var("BLOXROUTE_AUTH_HEADER").ok().filter(|s| !s.is_empty());

    let reqwest_client = Client::new();

    let (gas_tx, gas_rx) = watch::channel(GasState::default());

    // نونس تصاعدي آمن عبر AtomicU64 بدل watch channel ثابت
    let start_nonce = provider.get_transaction_count(my_address).await?;
    let nonce_counter = Arc::new(AtomicU64::new(start_nonce));

    {
        let nonce_watch = nonce_counter.clone();
        let provider_watch = provider.clone();
        let addr_watch = my_address;
        tokio::spawn(async move {
            const CHECK_INTERVAL: Duration = Duration::from_secs(8);
            const STALL_THRESHOLD: u32 = 4; // ~32 ثانية بدون أي تقدم = عالق فعليًا
            let mut stall_count: u32 = 0;
            let mut last_seen_chain_nonce: Option<u64> = None;

            loop {
                tokio::time::sleep(CHECK_INTERVAL).await;
                let Ok(chain_nonce) = provider_watch.get_transaction_count(addr_watch).await else {
                    continue;
                };
                let local_nonce = nonce_watch.load(Ordering::SeqCst);

                if local_nonce <= chain_nonce {
                    // ما في فجوة (أو الشبكة لحقت العدّاد المحلي) — كله طبيعي
                    stall_count = 0;
                    last_seen_chain_nonce = Some(chain_nonce);
                    continue;
                }

                match last_seen_chain_nonce {
                    Some(prev) if prev == chain_nonce => {
                        stall_count += 1;
                        if stall_count >= STALL_THRESHOLD {
                            warn!(
                                "⚠️ nunes is stuck={local_nonce} but the network is at {chain_nonce} \
                                 without progress for {}s — resetting the nonce counter to the network value \
                                 (any transactions in the gap will be ignored and need manual resubmission if still important)",
                                STALL_THRESHOLD * CHECK_INTERVAL.as_secs() as u32
                            );
                            nonce_watch.store(chain_nonce, Ordering::SeqCst);
                            stall_count = 0;
                        }
                    }
                    _ => stall_count = 0,
                }
                last_seen_chain_nonce = Some(chain_nonce);
            }
        });
    }

    let market_state: MarketState = Arc::new(RwLock::new(HashMap::new()));

    bootstrap_reserves_multicall(&provider, &known_pairs, &pair_registry, &market_state).await;

    let code_cache: CodeCache = Arc::new(RwLock::new(HashMap::new()));
    {
        let mut addresses: HashSet<Address> = HashSet::new();
        addresses.insert(my_contract);
        addresses.extend(known_routers.iter().copied());
        addresses.extend(known_pairs.iter().copied());
        for (t0, t1) in pair_registry.keys() {
            addresses.insert(*t0);
            addresses.insert(*t1);
        }
        warm_code_cache(&provider, &addresses, &code_cache).await;
    }

    // Thread 1: مزامنة الغاز والبلوكات
    {
        let provider_gas = provider.clone();
        tokio::spawn(async move {
            loop {
                if let Ok(fee_history) = provider_gas
                    .get_fee_history(1, BlockNumberOrTag::Latest, &[])
                    .await
                {
                    if let (Some(base_fee), Ok(block)) = (
                        fee_history.base_fee_per_gas.last(),
                        provider_gas.get_block_number().await,
                    ) {
                        let _ = gas_tx.send(GasState {
                            base_fee: U256::from(*base_fee),
                            current_block: block,
                        });
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
    }

    // Thread 2: مزامنة الـ reserves لحظيًا عبر أحداث Sync
    {
        let provider_sync = provider.clone();
        let state_sync = market_state.clone();
        let pairs_filter = known_pairs.clone();
        tokio::spawn(async move {
            let mut filter = Filter::new().event_signature(Sync::SIGNATURE_HASH);
            if !pairs_filter.is_empty() {
                filter = filter.address(pairs_filter.iter().copied().collect::<Vec<_>>());
            }
            loop {
                match provider_sync.subscribe_logs(&filter).await {
                    Ok(logs_sub) => {
                        let mut stream = logs_sub.into_stream();
                        while let Some(log) = stream.next().await {
                            update_reserve_from_log(&state_sync, &log).await;
                        }
                        warn!("Sync logs subscription disconnected, retrying...");
                    }
                    Err(e) => {
                        error!("failed to subscribe to Sync events: {e}");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        });
    }

    info!("🎧 iam sharing Mempool (full-tx subscription)...");
    let mut pending_tx_sub = provider.subscribe_full_pending_transactions().await?.into_stream();

    let sim_semaphore = Arc::new(Semaphore::new(sim_concurrency));

    loop {
        if let Some(target_tx) = pending_tx_sub.next().await {
            let provider_task = provider.clone();
            let gas_state = *gas_rx.borrow();
            let wallet_clone = wallet.clone();
            let req_client = reqwest_client.clone();
            let state_clone = market_state.clone();
            let routers = known_routers.clone();
            let pairs = known_pairs.clone();
            let registry = pair_registry.clone();
            let router_dex_task = router_dex.clone();
            let pair_dex_task = pair_dex.clone();
            let dex_factories_task = dex_factories.clone();
            let nonce_counter = nonce_counter.clone();
            let provider_nonce_resync = provider.clone();
            let semaphore = sim_semaphore.clone();
            let contract_addr = my_contract;
            let my_addr = my_address;
            let code_cache_task = code_cache.clone();
            let trade_log_task = trade_log.clone();
            let builders_task = builder_urls.clone();
            let bloxroute_auth_task = bloxroute_auth.clone();
            let bribe_bps_task = bribe_bps;
            let bundle_window_task = bundle_block_window;

            tokio::spawn(async move {
                let Ok(_permit) = semaphore.try_acquire() else {
                    return;
                };

                let to_address = match target_tx.to() {
                    Some(a) => a,
                    None => return,
                };

                if !routers.contains(&to_address) && !pairs.contains(&to_address) {
                    return;
                }

                // فك تشفير calldata الضحية (يشمل msg.value لصفقات ETH/MATIC)
                let victim_swap = match decode_victim_swap(target_tx.input(), target_tx.value()) {
                    Some(v) if v.amount_in > U256::ZERO && v.path.len() >= 2 => v,
                    _ => return,
                };

                info!("⚡ potential opportunity: {} amount_in={}", target_tx.tx_hash(), victim_swap.amount_in);

                let (pair1, pair2, factory1, factory2, optimal_borrow, snap1, snap2, dex1_id, dex2_id) = {
                    let state = state_clone.read().await;
                    let (victim_pair, victim_dex, ref_pair, ref_dex) = match resolve_pair_pair(
                        &registry,
                        &pair_dex_task,
                        &router_dex_task,
                        &state,
                        to_address,
                        &victim_swap,
                    ) {
                        Ok(p) => p,
                        Err(reason) => {
                    debug!(
                         "❌ [PAIR_RESOLVE_MISS] tx={} to={} token0={} token1={} reason={}",
                         target_tx.tx_hash(),
                         to_address,
                         victim_swap.path.first().copied().unwrap_or_default(),
                         victim_swap.path.get(1).copied().unwrap_or_default(),
                         reason
                        );                            return;
                        }
                    };
                    let (pair1, dex1, pair2, dex2) = (ref_pair, ref_dex, victim_pair, victim_dex);
                    let Some(&factory1) = dex_factories_task.get(&dex1) else {
                        warn!("no factory registered in DEX_FACTORIES for dex={dex1} (pair1={pair1})");
                        return;
                    };
                    let Some(&factory2) = dex_factories_task.get(&dex2) else {
                        warn!("nothing factory registered in DEX_FACTORIES for dex={dex2} (pair2={pair2})");
                        return;
                    };
                    let optimal_borrow = match compute_optimal_backrun_amount(&state, pair1, pair2, &victim_swap) {
                        Some(v) if v > U256::ZERO => v,
                        _ => {
                            debug!("❌ dropped @ compute_optimal_backrun_amount (no positive borrow) for tx {}", target_tx.tx_hash());
                            return;
                        }
                    };
                    let Some(&snap1) = state.get(&pair1) else {
                        debug!("❌ dropped @ missing snap1 in state for pair1={pair1}");
                        return;
                    };
                    let Some(&snap2) = state.get(&pair2) else {
                        debug!("❌ dropped @ missing snap2 in state for pair2={pair2}");
                        return;
                    };
                    (pair1, pair2, factory1, factory2, optimal_borrow, snap1, snap2, dex1, dex2)
                };

                let deadline = U256::from(now_unix() + 15);

                let call_data = match build_calldata(
                    pair1, pair2, factory1, factory2, deadline,
                    &victim_swap, optimal_borrow, &snap1, &snap2,
                ) {
                    Some(c) => c,
                    None => {
                        debug!("❌ dropped @ build_calldata for tx {}", target_tx.tx_hash());
                        return;
                    }
                };

                let repay_token = victim_swap.path[0];
                let quick_profit = match estimate_arb_profit(&snap1, &snap2, &victim_swap, optimal_borrow) {
                    Some(p) => p,
                    None => {
                        debug!("❌ dropped @ estimate_arb_profit (None) for tx {}", target_tx.tx_hash());
                        return;
                    }
                };
                let quick_gas_floor_wei =
                    U256::from(FAST_FILTER_GAS_ESTIMATE).saturating_mul(gas_state.base_fee);
                let quick_gas_floor = {
                    let state = state_clone.read().await;
                    match gas_cost_in_token(&registry, &state, repay_token, quick_gas_floor_wei) {
                        Some(v) => v,
                        None => {
                            debug!("❌ dropped @ gas_cost_in_token (no WMATIC/{repay_token} pool)");
                            return; // ما لقينا pool WMATIC/repay_token — نرفض بدل تخمين
                        }
                    }
                };
                if quick_profit <= quick_gas_floor {
                    debug!("❌ dropped @ quick_profit filter: profit={quick_profit} <= gas_floor={quick_gas_floor}");
                    return;
                }

                let my_nonce_hint = nonce_counter.load(Ordering::SeqCst);
                let provider_sim = provider_task.clone();
                let target_tx_sim = target_tx.clone();
                let call_data_sim = call_data.clone();
                let code_cache_sim = code_cache_task.clone();
                let sim_result = match tokio::task::spawn_blocking(move || {
                    let rt = tokio::runtime::Handle::current();
                    rt.block_on(async {
                        simulate_bundle_revm(
                            &provider_sim,
                            &target_tx_sim,
                            contract_addr,
                            my_addr,
                            &call_data_sim,
                            repay_token,
                            &code_cache_sim,
                            my_nonce_hint,
                        )
                        .await
                    })
                })
                .await
                {
                    Ok(Ok(r)) => r,
                    Ok(Err(e)) => {
                        warn!("simulation failed REVM: {e}");
                        return;
                    }
                    Err(e) => {
                        warn!("spawn_blocking failed: {e}");
                        return;
                    }
                };
                if !sim_result.is_profitable {
                    debug!("❌ dropped @ REVM sim: not profitable (expected_profit={}, gas_used={})", sim_result.expected_profit, sim_result.gas_used);
                    return;
                }

                let base_gas_cost_wei = U256::from(sim_result.gas_used).saturating_mul(gas_state.base_fee);
                    let base_gas_cost = {
                        let state = state_clone.read().await;
                        match gas_cost_in_token(&registry, &state, repay_token, base_gas_cost_wei) {
                            Some(v) => v,
                            None => return,
                        }
                    };
                    if sim_result.expected_profit <= base_gas_cost {
                        debug!("❌ dropped @ final gas check: expected_profit={} <= base_gas_cost={}", sim_result.expected_profit, base_gas_cost);
                        return;
                    }
                    info!("✅ PASSED all filters, proceeding to build+sign+submit tx (repay_token={repay_token})");

                let net_profit = sim_result.expected_profit - base_gas_cost;

                let miner_bribe = net_profit
                    .checked_mul(U256::from(bribe_bps_task))
                    .and_then(|v| v.checked_div(U256::from(10_000u64)))
                    .unwrap_or(U256::ZERO);

                if sim_result.gas_used == 0 {
                    return;
                }
                let priority_fee_per_gas = miner_bribe / U256::from(sim_result.gas_used);
                debug!(
                    "bribe calc: net_profit={net_profit} bribe_bps={bribe_bps_task} miner_bribe={miner_bribe} gas_used={} priority_fee_per_gas={priority_fee_per_gas}",
                    sim_result.gas_used
                );

                // هامش أمان على الغاز (20%) حتى لا يفشل التنفيذ الفعلي لاختلاف بسيط عن المحاكاة
                let gas_limit = sim_result.gas_used.saturating_mul(120) / 100;

                let my_nonce = nonce_counter.fetch_add(1, Ordering::SeqCst);

                let my_backrun_tx = TransactionRequest::default()
                    .with_to(contract_addr)
                    .with_nonce(my_nonce)
                    .with_input(call_data)
                    .with_gas_limit(gas_limit)
                    .with_max_fee_per_gas((gas_state.base_fee + priority_fee_per_gas).to::<u128>())
                    .with_max_priority_fee_per_gas(priority_fee_per_gas.to::<u128>());

                let signed_envelope = match my_backrun_tx.build(&wallet_clone).await {
                    Ok(tx) => tx,
                    Err(e) => {
                        error!("failed to sign transaction with nonce {my_nonce}: {e} — this will create a nonce gap, syncing with the network");
                        // do not try to "recover" the nonce manually (not safe under high concurrency —
                        // other tasks might have consumed higher numbers already). instead, reset
                        // the nonce counter entirely from the actual network value, which is the highest
                        // safe value we can ensure won't be reused from another task.
                        if let Ok(chain_nonce) = provider_nonce_resync.get_transaction_count(my_addr).await {
                            let _ = nonce_counter.fetch_max(chain_nonce, Ordering::SeqCst);
                        }
                        return;
                    }
                };

                let our_tx_hash = *signed_envelope.tx_hash();
                let victim_tx_hash = target_tx.tx_hash();

                let target_block = gas_state.current_block + 1;
                let max_block = gas_state.current_block + bundle_window_task;
                let bundle = serde_json::json!({
                    "txs": [
                        format!("0x{}", alloy::hex::encode(target_tx.inner.encoded_2718())),
                        format!("0x{}", alloy::hex::encode(signed_envelope.encoded_2718())),
                    ],
                    "blockNumber": format!("0x{:x}", target_block),
                    "maxBlockNumber": format!("0x{:x}", max_block),
                });

                let builder_count = builders_task.len() as u32;
                broadcast_bundle_to_polygon(
                    &req_client,
                    &builders_task,
                    bundle,
                    bloxroute_auth_task.as_deref(),
                )
                .await;

                record_submitted_trade(
                    &trade_log_task,
                    our_tx_hash,
                    victim_tx_hash,
                    sim_result.expected_profit,
                    base_gas_cost,
                    &dex1_id,
                    &dex2_id,
                    builder_count,
                )
                .await;

                let provider_watch = provider_task.clone();
                let log_watch = trade_log_task.clone();
                tokio::spawn(async move {
                    watch_trade_outcome(provider_watch, log_watch, our_tx_hash, sim_result.gas_used).await;
                });
            });
        }
    }
}

// ==========================================
// بنية بيانات وسيطة لصفقة الضحية بعد فك التشفير
// ==========================================
#[derive(Debug, Clone)]
struct VictimSwap {
    amount_in: U256,
    path: Vec<Address>,
}

/// يحاول فك تشفير أشهر دوال سواب على راوتر Uniswap V2 / QuickSwap.
/// أرجع None إذا الـ selector مش معروف عندنا (تجاهل المعاملة بسرعة).
/// كامل)، منقارن أول 4 بايت (function selector) مرة وحدة ومنروح مباشرة للدالة الصحيحة.
/// معاملات mempool اللي مش من دوالنا المعروفة بترجع None فورًا بدون أي محاولة فك تشفير.
fn decode_victim_swap(input: &Bytes, tx_value: U256) -> Option<VictimSwap> {
    use IUniswapV2Router::*;

    if input.len() < 4 {
        return None;
    }
    let selector: [u8; 4] = input[0..4].try_into().ok()?;

    if selector == swapExactTokensForTokensCall::SELECTOR {
        let call = swapExactTokensForTokensCall::abi_decode(input).ok()?;
        return Some(VictimSwap { amount_in: call.amountIn, path: call.path });
    }
    if selector == swapExactTokensForETHCall::SELECTOR {
        let call = swapExactTokensForETHCall::abi_decode(input).ok()?;
        return Some(VictimSwap { amount_in: call.amountIn, path: call.path });
    }
    if selector == swapExactETHForTokensCall::SELECTOR {
        let call = swapExactETHForTokensCall::abi_decode(input).ok()?;
        return Some(VictimSwap { amount_in: tx_value, path: call.path });
    }
    if selector == swapExactTokensForTokensSupportingFeeOnTransferTokensCall::SELECTOR {
        let call = swapExactTokensForTokensSupportingFeeOnTransferTokensCall::abi_decode(input).ok()?;
        return Some(VictimSwap { amount_in: call.amountIn, path: call.path });
    }
    None
}

/// يطبّق أثر صفقة الضحية على reserves المجمّع بالاتجاه الصحيح (token_in → token_out).
fn apply_victim_effect(
    pool: &PoolState,
    token_in: Address,
    token_out: Address,
    amount_in: U256,
) -> Option<(U256, U256)> {
    let gamma = U256::from(10_000u64.saturating_sub(pool.fee_bps as u64));
    if pool.token0 == token_in && pool.token1 == token_out {
        let (r0, r1) = apply_swap_effect(pool.reserve0, pool.reserve1, amount_in, gamma)?;
        Some((r0, r1))
    } else if pool.token1 == token_in && pool.token0 == token_out {
        let (r_out, r_in) = apply_swap_effect(pool.reserve1, pool.reserve0, amount_in, gamma)?;
        Some((r_in, r_out))
    } else {
        None
    }
}

/// يحوّل reserves إلى (x,y) حيث x=repay_token و y=borrow_token — لصيغة المراجحة.
fn orient_reserves(
    pool: &PoolState,
    repay_token: Address,
    borrow_token: Address,
    r0: U256,
    r1: U256,
) -> Option<(U256, U256)> {
    if pool.token0 == repay_token && pool.token1 == borrow_token {
        Some((r0, r1))
    } else if pool.token1 == repay_token && pool.token0 == borrow_token {
        Some((r1, r0))
    } else {
        None
    }
}

/// مبلغ repay_token المطلوب لسداد flash swap من pair1 (صيغة getAmountIn القياسية V2 = 0.3%).
fn flash_repay_amount(
    pool: &PoolState,
    borrow_token: Address,
    repay_token: Address,
    r0: U256,
    r1: U256,
    borrow_amount: U256,
) -> Option<U256> {
    let gamma = U256::from(10_000u64.saturating_sub(pool.fee_bps as u64));
    // لازم الريزيرفين يبقوا بقيمتهم الأصلية (بدون أي طرح مسبق) — get_amount_in
    // بتطرح amount_out من reserve_out جوّاها بنفسها. reserve_in هون هو ريزيرف
    // توكن السداد (اللي رح تدفعه)، وreserve_out هو ريزيرف توكن الاقتراض.
    let (reserve_borrow, reserve_repay) = if pool.token0 == borrow_token && pool.token1 == repay_token {
        (r0, r1)
    } else if pool.token1 == borrow_token && pool.token0 == repay_token {
        (r1, r0)
    } else {
        return None;
    };
    get_amount_in(borrow_amount, reserve_repay, reserve_borrow, gamma)
}

/// تقدير ربح سريع (بدون REVM) — نفس مسار العقد: اقتراض → سواب pair2 → سداد pair1.
fn estimate_arb_profit(
    pool1: &PoolState, // مصدر الاقتراض (flash source) — بدون أثر صفقة الضحية
    pool2: &PoolState, // وجهة البيع — هذا هو الـ pool يلي فعلاً تحرّك بصفقة الضحية
    victim: &VictimSwap,
    borrow_amount: U256,
) -> Option<U256> {
    let token_in = victim.path.first().copied()?;
    let token_out = victim.path.get(1).copied()?;
    let repay_needed = flash_repay_amount(pool1, token_out, token_in, pool1.reserve0, pool1.reserve1, borrow_amount)?;
    let (r0v, r1v) = apply_victim_effect(pool2, token_in, token_out, victim.amount_in)?;
    let gamma2 = U256::from(10_000u64.saturating_sub(pool2.fee_bps as u64));
    let (rin2, rout2) = if pool2.token0 == token_out && pool2.token1 == token_in {
        (r0v, r1v)
    } else if pool2.token1 == token_out && pool2.token0 == token_in {
        (r1v, r0v)
    } else {
        return None;
    };
    let out_repay = get_amount_out(borrow_amount, rin2, rout2, gamma2)?;
    out_repay.checked_sub(repay_needed)
}
/// يحوّل تكلفة غاز (wei من MATIC) لمكافئها بوحدة أي توكن، عبر أقرب pool WMATIC/token
/// بالـ market_state (ما في price oracle، فبنستخدم الـ reserves كمرجع).
fn gas_cost_in_token(
    registry: &PairRegistry,
    state: &HashMap<Address, PoolState>,
    token: Address,
    gas_cost_wei: U256,
) -> Option<U256> {
    if token == WMATIC {
        return Some(gas_cost_wei); // WMATIC ~1:1 مع MATIC
    }
    let candidates = registry.get(&sorted_pair(WMATIC, token))?;
    let pool_addr = candidates.first()?.0;
    let pool = state.get(&pool_addr)?;
    let (reserve_wmatic, reserve_token) = if pool.token0 == WMATIC {
        (pool.reserve0, pool.reserve1)
    } else if pool.token1 == WMATIC {
        (pool.reserve1, pool.reserve0)
    } else {
        return None;
    };
    let gamma = U256::from(10_000u64.saturating_sub(pool.fee_bps as u64));
    get_amount_out(gas_cost_wei, reserve_wmatic, reserve_token, gamma)
}
/// السعر الضمني للتوكن1 بدلالة التوكن0 (مقياس ثابت للمقارنة بين مجمعين، مضروب بـ 1e18
/// لتفادي الكسور بحساب الأعداد الصحيحة).
fn implied_price_1e18(reserve0: U256, reserve1: U256) -> Option<U256> {
    if reserve0 == U256::ZERO {
        return None;
    }
    reserve1.checked_mul(U256::from(10u64).pow(U256::from(18u64)))?.checked_div(reserve0)
}

/// يحدد pair1 (المجمّع اللي الضحية تداولت عليه فعليًا، عبر مطابقة dexId) وpair2
/// (مجمّع آخر لنفس زوج التوكنات، أبعد سعريًا، مرشّح للمراجحة). يرفض الفرصة صراحة
/// لو الـ dex غير معروف بدل تخمين قد يبني صفقة على pair غلط بالكامل.
fn resolve_pair_pair(
    registry: &PairRegistry,
    pair_dex: &PairDexMap,
    router_dex: &RouterDexMap,
    state: &HashMap<Address, PoolState>,
    to_address: Address,
    victim: &VictimSwap,
) -> Result<(Address, String, Address, String), &'static str> {
    if victim.path.len() < 2 {
        return Err("victim path too short");
    }
    let token_pair = sorted_pair(victim.path[0], victim.path[1]);
    let candidates = registry.get(&token_pair).ok_or("token pair not in registry")?;
    if candidates.len() < 2 {
        // ما في DEX تاني مسجّل لنفس الزوج، ما في فرصة مراجحة (طبيعي جدًا وشائع
        // على mempool حقيقي — أغلب الأزواج موجودة بمكان واحد بس بإعداداتك)
        return Err("only one DEX known for this pair, no cross-dex arb possible");
    }

    // معرّف الـ DEX اللي فعليًا نفّذت عليه الضحية صفقتها
    // عنوانه اجتاز الفلترة المبكرة (routers/pairs sets) لكن ما انربط بـ DEX id —
    // فحص إعدادات router_dex_task/pair_dex_task تبعتك، غالبًا نسيت تسجّل هالراوتر/الزوج.
    let victim_dex_id = pair_dex
        .get(&to_address)
        .or_else(|| router_dex.get(&to_address))
        .ok_or("to_address not mapped to a known dex id")?;

    let (pair1, dex1) = candidates
        .iter()
        .find(|(_, dex)| dex == victim_dex_id)
        .map(|(addr, dex)| (*addr, dex.clone()))
        .ok_or("no registered pair matches victim's dex id for this token pair")?;

    // إما bootstrap_reserves_multicall ما غطّى هالمجمّع، أو ما وصل أي حدث Sync له لسا.
    let p1 = state.get(&pair1).ok_or("pair1 reserves not loaded in market state yet")?;
    let price1 = implied_price_1e18(p1.reserve0, p1.reserve1).ok_or("pair1 price calc failed (zero reserve?)")?;

    // pair2 = أكبر انحراف سعري عن pair1 من بين باقي المجمعات المسجّلة لنفس التوكنين
    // (على أي DEX تاني غير dex الضحية — هدف المراجحة أصلًا هو استغلال فرق سعر بين DEXين)
    let (pair2, dex2) = candidates
        .iter()
        .filter(|(p, _)| *p != pair1)
        .filter_map(|(p, dex)| {
            let pool = state.get(p)?;
            let price = implied_price_1e18(pool.reserve0, pool.reserve1)?;
            let diff = price.abs_diff(price1);
            Some((*p, dex.clone(), diff))
        })
        .max_by_key(|(_, _, diff)| *diff)
        .map(|(p, dex, _)| (p, dex))
        .ok_or("no other candidate pair has loaded reserves yet")?;

    Ok((pair1, dex1, pair2, dex2))
}

/// أفضل مبلغ اقتراض للمراجحة بين pair1 (مصدر القرض، غير متأثر) وpair2 (تأثر
/// بصفقة الضحية)، عبر ternary search مباشر فوق دالة الربح الحقيقية (نفس الصيغ
/// اللي بيستخدمها العقد: get_amount_out + flash_repay_amount).
fn compute_optimal_backrun_amount(
    state: &HashMap<Address, PoolState>,
    pair1: Address,
    pair2: Address,
    victim: &VictimSwap,
) -> Option<U256> {
    let p1 = state.get(&pair1)?;
    let p2 = state.get(&pair2)?;
    let token_in = victim.path.first().copied()?;
    let token_out = victim.path.get(1).copied()?;

    let (x1, y1) = orient_reserves(p1, token_in, token_out, p1.reserve0, p1.reserve1)?;
    let (r0v, r1v) = apply_victim_effect(p2, token_in, token_out, victim.amount_in)?;
    let (x2, y2) = orient_reserves(p2, token_in, token_out, r0v, r1v)?;

    let gamma1_num = U256::from(10_000u64.saturating_sub(p1.fee_bps as u64));
    let gamma2_num = U256::from(10_000u64.saturating_sub(p2.fee_bps as u64));
    let _ = (x1, y1, x2, y2, gamma1_num, gamma2_num); // مستخدمة داخل profit_at أدناه عبر p1/p2

    let profit_at = |l: U256| -> Option<U256> {
        if l == U256::ZERO || l >= y1 {
            return None;
        }
        let sold_out = get_amount_out(l, y2, x2, gamma2_num)?; // بيع l من borrow_token بـ p2 → repay_token
        let repay = flash_repay_amount(p1, token_out, token_in, p1.reserve0, p1.reserve1, l)?;
        sold_out.checked_sub(repay)
    };

    let mut lo = U256::from(1u64);
    let mut hi = y1.saturating_sub(U256::from(1u64));
    if hi <= lo {
        return None;
    }
    for _ in 0..200 {
        if hi <= lo {
            break;
        }
        let third = (hi - lo) / U256::from(3u64);
        if third == U256::ZERO {
            break;
        }
        let m1 = lo + third;
        let m2 = hi - third;
        let p_m1 = profit_at(m1).unwrap_or(U256::ZERO);
        let p_m2 = profit_at(m2).unwrap_or(U256::ZERO);
        if p_m1 < p_m2 {
            lo = m1;
        } else {
            hi = m2;
        }
    }

    let optimal = (lo + hi) / U256::from(2u64);
    let p = profit_at(optimal);
    debug!(
        "🔍 backrun calc: pair1={pair1} pair2={pair2} optimal={optimal} profit={:?} y1={y1} y2={y2}",
        p
    );
    if optimal == U256::ZERO {
        return None;
    }
    // تأكيد أن التقدير السريع يقول في ربح — وإلا المبلغ "الأمثل" غير قابل للتنفيذ
    match p {
        Some(pv) if pv > U256::ZERO => Some(optimal),
        _ => None,
    }
}

fn get_amount_out(amount_in: U256, reserve_in: U256, reserve_out: U256, gamma_num: U256) -> Option<U256> {
    if amount_in == U256::ZERO {
        return Some(U256::ZERO);
    }
    let amount_in_with_fee = amount_in.checked_mul(gamma_num)?;
    let numerator = amount_in_with_fee.checked_mul(reserve_out)?;
    let denominator = reserve_in.checked_mul(U256::from(10_000u64))?.checked_add(amount_in_with_fee)?;
    let amount_out = numerator.checked_div(denominator)?;
    if amount_out >= reserve_out {
        return None;
    }
    Some(amount_out)
}

/// صيغة getAmountIn القياسية لـ Uniswap V2 (تشمل رسوم 0.3%).
fn get_amount_in(amount_out: U256, reserve_in: U256, reserve_out: U256, gamma_num: U256) -> Option<U256> {
    if amount_out == U256::ZERO {
        return Some(U256::ZERO);
    }
    if amount_out >= reserve_out {
        return None;
    }
    let numerator = reserve_in
        .checked_mul(amount_out)?
        .checked_mul(U256::from(10_000u64))?;
    let denominator = reserve_out
        .checked_sub(amount_out)?
        .checked_mul(gamma_num)?;
    Some(numerator.checked_div(denominator)?.saturating_add(U256::from(1u64)))
}

/// يحاكي أثر سواب بسيط (constant product مع رسوم) على الـ reserves ويرجّع الحالة الجديدة.
fn apply_swap_effect(reserve_in: U256, reserve_out: U256, amount_in: U256, gamma_num: U256) -> Option<(U256, U256)> {
    if amount_in == U256::ZERO {
        return Some((reserve_in, reserve_out));
    }
    let amount_out = get_amount_out(amount_in, reserve_in, reserve_out, gamma_num)?;
    Some((reserve_in + amount_in, reserve_out - amount_out))
}

/// جذر تربيعي صحيح لـ U256 عبر خوارزمية Newton (Babylonian method)
fn isqrt_u256(n: U256) -> U256 {
    if n == U256::ZERO {
        return U256::ZERO;
    }
    let mut x = n;
    let mut y = (x + U256::from(1u64)) / U256::from(2u64);
    while y < x {
        x = y;
        y = (x + n / x) / U256::from(2u64);
    }
    x
}

async fn update_reserve_from_log(state: &MarketState, log: &Log) {
    let pool_address = log.address();
    let Some(data) = log.data().data.as_ref().get(0..64) else { return };

    // Sync(uint112 reserve0, uint112 reserve1) — كل واحد مربوط بـ 32 بايت داخل الـ ABI encoding
    let reserve0 = U256::from_be_slice(&data[0..32]);
    let reserve1 = U256::from_be_slice(&data[32..64]);

    let mut map = state.write().await;
    let entry = map.entry(pool_address).or_insert(PoolState {
        reserve0: U256::ZERO,
        reserve1: U256::ZERO,
        fee_bps: 30,
        ..Default::default()
    });
    entry.reserve0 = reserve0;
    entry.reserve1 = reserve1;
}

async fn bootstrap_reserves_multicall<P: Provider>(
    provider: &Arc<P>,
    known_pairs: &HashSet<Address>,
    registry: &PairRegistry,
    state: &MarketState,
) {
    if known_pairs.is_empty() {
        return;
    }

    let mut pair_tokens: HashMap<Address, (Address, Address)> = HashMap::new();
    for (&tokens, pairs) in registry.iter() {
        for (p, _dex_id) in pairs {
            pair_tokens.insert(*p, tokens);
        }
    }

    let calls: Vec<IMulticall3::Call3> = known_pairs
        .iter()
        .map(|&pair| IMulticall3::Call3 {
            target: pair,
            allowFailure: true,
            callData: IUniswapV2Pair::getReservesCall {}.abi_encode().into(),
        })
        .collect();

    let ordered_pairs: Vec<Address> = known_pairs.iter().copied().collect();
    let call = IMulticall3::aggregate3Call { calls };

    let tx = TransactionRequest::default()
        .with_to(MULTICALL3)
        .with_input(call.abi_encode());

    let result = match provider.call(tx).await {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("failed to call Multicall3 for initial reserve loading: {e}. Bot will initialize with zero values and wait for the first Sync event.");
            return;
        }
    };

    let Ok(decoded) = IMulticall3::aggregate3Call::abi_decode_returns(&result) else {
        error!("failed to decode Multicall3 response");
        return;
    };

    let mut map = state.write().await;
    for (pair, res) in ordered_pairs.into_iter().zip(decoded.iter()) {
        if !res.success {
            continue;
        }
        let Ok(reserves) = IUniswapV2Pair::getReservesCall::abi_decode_returns(&res.returnData) else {
            continue;
        };
        let (token0, token1) = pair_tokens.get(&pair).copied().unwrap_or_default();
        map.insert(
            pair,
            PoolState {
                reserve0: U256::from(reserves.reserve0),
                reserve1: U256::from(reserves.reserve1),
                fee_bps: 30,
                token0,
                token1,
            },
        );
    }
    info!("✅   initial download for {}  across complex Multicall3", map.len());
}

/// يجلب bytecode كل عنوان بالقائمة عبر eth_getCode (طلبات متوازية) ويخزّنه بالـ CodeCache.
/// يُستدعى مرة وحدة عند الإقلاع فقط — bytecode العقود المنشورة لا يتغيّر عمليًا (ما لم
/// يكن عقد proxy قابل للترقية، وهاد نادر لراوترات/أزواج AMM القياسية).
async fn warm_code_cache<P: Provider>(
    provider: &Arc<P>,
    addresses: &HashSet<Address>,
    cache: &CodeCache,
) {
    let mut tasks = Vec::with_capacity(addresses.len());
    for &addr in addresses {
        let provider = provider.clone();
        tasks.push(async move {
            let code = provider.get_code_at(addr).await.ok();
            (addr, code)
        });
    }
    let results = futures_util::future::join_all(tasks).await;

    let mut map = cache.write().await;
    let mut loaded = 0usize;
    for (addr, code) in results {
        if let Some(code) = code {
            if !code.is_empty() {
                map.insert(addr, code);
                loaded += 1;
            }
        }
    }
    info!("🔥 to equip bytecode: {loaded}/{} addresses", addresses.len());
}


async fn simulate_bundle_revm<P: Provider + Clone + 'static>(
    provider: &Arc<P>,
    target_tx: &Transaction,
    my_contract: Address,
    my_address: Address,
    my_calldata: &Bytes,
    profit_token: Address,
    code_cache: &CodeCache,
    my_nonce: u64,
) -> Result<SimulationResult, String> {
    let alloy_db = AlloyDB::new(provider.clone(), BlockNumberOrTag::Latest.into());
    let wrapped_db = WrapDatabaseAsync::new(alloy_db)
        .ok_or("WrapDatabaseAsync: requires tokio runtime with multiple threads (multi-thread)")?;
    let mut cache_db = CacheDB::new(wrapped_db);

 
    {
        let warm = code_cache.read().await;
        for (&addr, code) in warm.iter() {
            let bytecode = Bytecode::new_raw(code.clone().into());
            let info = AccountInfo {
                balance: U256::ZERO,
                nonce: 0,
                code_hash: bytecode.hash_slow(),
                code: Some(bytecode),
                ..Default::default() // ⚡ v6.5: يغطي أي حقول جديدة مستقبلية (مثل account_id بإصدار 41)
            };
            cache_db.insert_account_info(addr, info);
        }
    }

    // دالة مساعدة صغيرة لقراءة balanceOf عبر eth_call مباشرة (أبسط وأسرع من قراءة
    // الـ storage slot يدويًا لأن slot الرصيد بيختلف حسب تطبيق العقد).
    let balance_call = IERC20::balanceOfCall { account: my_address };
    let balance_of_calldata: Bytes = balance_call.abi_encode().into();

    let mut evm = Context::mainnet()
        .with_db(&mut cache_db)
        .build_mainnet();

    let onchain_nonce = provider.get_transaction_count(target_tx.from()).await.unwrap_or(target_tx.nonce());
    let victim_already_mined = onchain_nonce > target_tx.nonce();

    if !victim_already_mined {
        let victim_tx = TxEnv {
            caller: target_tx.from(),
            kind: TxKind::Call(target_tx.to().unwrap_or_default()),
            data: RevmBytes::copy_from_slice(target_tx.input()),
            value: target_tx.value(),
            gas_limit: target_tx.gas_limit(),
            gas_price: TransactionResponse::gas_price(target_tx).unwrap_or_default(),
            nonce: target_tx.nonce(),
            ..Default::default()
        };

        let victim_result = evm.transact_commit(victim_tx).map_err(|e| e.to_string())?; // commit: لازم نطبّق أثرها فعليًا على الحالة قبل معاملتنا
        if !victim_result.is_success() {
            debug!("❌ sim aborted: victim tx reverted (from={}, to={:?})", target_tx.from(), target_tx.to());
            return Ok(SimulationResult { is_profitable: false, expected_profit: U256::ZERO, gas_used: 0 });
        }
    } else {
        debug!("ℹ️ victim tx already mined on-chain (nonce {} <= chain nonce {}), skipping manual replay", target_tx.nonce(), onchain_nonce);
    }
    drop(evm);

    // رصيد profitToken قبل معاملتنا (بعد أثر الضحية)
    let balance_before = read_erc20_balance(&mut cache_db, profit_token, &balance_of_calldata).map_err(|e| e.to_string())?;

    // === خطوة 2: نفّذ معاملتنا (executeFlashArbitrage) فوق نفس الحالة ===
    let mut evm2 = Context::mainnet()
        .with_db(&mut cache_db)
        .build_mainnet();

    let my_tx = TxEnv {
        caller: my_address,
        kind: TxKind::Call(my_contract),
        data: RevmBytes::copy_from_slice(my_calldata),
        value: U256::ZERO,
        gas_limit: 2_000_000,
        nonce: my_nonce,
        ..Default::default()
    };

    let my_result = evm2.transact_commit(my_tx).map_err(|e| e.to_string())?;
    drop(evm2);

    if !my_result.is_success() {
        let gas_spent = my_result.tx_gas_used();
        let revert_data = my_result.output().cloned().unwrap_or_default();
        debug!(
            "❌ sim aborted: our tx reverted (gas_used={}, output=0x{})",
            gas_spent,
            alloy::hex::encode(&revert_data)
        );
        return Ok(SimulationResult { is_profitable: false, expected_profit: U256::ZERO, gas_used: gas_spent });
    }
    let gas_used = my_result.tx_gas_used();

    let balance_after = read_erc20_balance(&mut cache_db, profit_token, &balance_of_calldata).map_err(|e| e.to_string())?;
    let expected_profit = balance_after.saturating_sub(balance_before);

    Ok(SimulationResult {
        is_profitable: expected_profit > U256::ZERO,
        expected_profit,
        gas_used,
    })
}

/// قراءة balanceOf عبر تنفيذ eth_call وهمي (staticcall-style) داخل نفس الـ cache_db،
/// بدون تغيير أي حالة (transact بدون commit).
fn read_erc20_balance<DB>(
    cache_db: &mut CacheDB<DB>,
    token: Address,
    calldata: &Bytes,
) -> Result<U256, Box<dyn std::error::Error>>
where
    DB: revm::DatabaseRef,
    DB::Error: std::fmt::Debug + std::error::Error + Send + std::marker::Sync + 'static,
{
    let mut evm = Context::mainnet()
        .with_db(cache_db)
        .build_mainnet();

    let tx = TxEnv {
        kind: TxKind::Call(token),
        data: RevmBytes::copy_from_slice(calldata),
        gas_limit: 200_000,
        ..Default::default()
    };

    let result = evm.transact(tx)?; // بدون commit — قراءة فقط
    let output = result.result.output().cloned().unwrap_or_default();
    if output.len() < 32 {
        return Ok(U256::ZERO);
    }
    Ok(U256::from_be_slice(&output[0..32]))
}

fn build_calldata(
    pair1: Address,
    pair2: Address,
    factory1: Address,
    factory2: Address,
    deadline: U256,
    victim: &VictimSwap,
    optimal_borrow: U256,
    pool1: &PoolState,
    pool2: &PoolState,
) -> Option<Bytes> {
    let token_in = *victim.path.first()?;
    let token_out = *victim.path.get(1)?;
    let borrow_token = token_out;
    let repay_token = token_in;
    let profit_token = token_in;

    let repay_amount = flash_repay_amount(pool1, borrow_token, repay_token, pool1.reserve0, pool1.reserve1, optimal_borrow)?;

    let is_pair1_borrow_token0 = pool1.token0 == borrow_token;
    if !is_pair1_borrow_token0 && pool1.token1 != borrow_token {
        return None;
    }

    let (amount0_out_pair1, amount1_out_pair1) = if is_pair1_borrow_token0 {
        (optimal_borrow, U256::ZERO)
    } else {
        (U256::ZERO, optimal_borrow)
    };

    let (r0v2, r1v2) = apply_victim_effect(pool2, token_in, token_out, victim.amount_in)?;
    let gamma2 = U256::from(10_000u64.saturating_sub(pool2.fee_bps as u64));
    let (reserve_in2, reserve_out2) = if pool2.token0 == borrow_token && pool2.token1 == repay_token {
        (r0v2, r1v2)
    } else if pool2.token1 == borrow_token && pool2.token0 == repay_token {
        (r1v2, r0v2)
    } else {
        return None;
    };
    let amount_out_pair2 = get_amount_out(optimal_borrow, reserve_in2, reserve_out2, gamma2)?;
    if amount_out_pair2 < repay_amount {
        return None;
    }

    let (amount0_out_pair2, amount1_out_pair2) = if pool2.token0 == repay_token {
        (amount_out_pair2, U256::ZERO)
    } else {
        (U256::ZERO, amount_out_pair2)
    };

    let params = ISuperSecureArbitrage::ArbParams {
        pair1,
        pair2,
        factory1,
        factory2,
        borrowToken: borrow_token,
        borrowAmount: optimal_borrow,
        amount0OutPair2: amount0_out_pair2,
        amount1OutPair2: amount1_out_pair2,
        repayToken: repay_token,
        repayAmount: repay_amount,
        profitToken: profit_token,
        minProfit: (amount_out_pair2.saturating_sub(repay_amount) / U256::from(10u64)).max(U256::from(1u64)),
        deadline,
    };

    let call = ISuperSecureArbitrage::executeFlashArbitrageCall {
        params,
        amount0OutPair1: amount0_out_pair1,
        amount1OutPair1: amount1_out_pair1,
    };

    Some(call.abi_encode().into())
}

async fn broadcast_bundle_to_polygon(
    client: &Client,
    builders: &[String],
    bundle: serde_json::Value,
    bloxroute_auth: Option<&str>,
) {
    let payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_sendBundle",
        "params": [bundle]
    });

    let mut tasks = vec![];
    for builder_url in builders {
        let client_clone = client.clone();
        let payload_clone = payload.clone();
        let url = builder_url.clone();
        let auth = bloxroute_auth.map(|s| s.to_string());
        tasks.push(tokio::spawn(async move {
            let mut req = client_clone.post(&url).json(&payload_clone);
            if url.contains("blxrbdn") {
                if let Some(ref header_val) = auth {
                    req = req.header("Authorization", header_val.as_str());
                }
            }
            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    if status.is_success() && !body.contains("\"error\"") {
                        info!("✅ bundle accepted by {url}: {body}");
                    } else {
                        warn!("⚠️ builder {url} ({status}): {body}");
                    }
                }
                Err(e) => warn!("فشل الإرسال إلى {url}: {e}"),
            }
        }));
    }
    futures_util::future::join_all(tasks).await;
    info!("🚀 sent Bundle to {} builder(s)", builders.len());
}
