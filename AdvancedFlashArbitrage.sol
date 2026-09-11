// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import "@openzeppelin/contracts/utils/ReentrancyGuard.sol";

interface IUniswapV2Pair {
    function swap(uint amount0Out, uint amount1Out, address to, bytes calldata data) external;
    function factory() external view returns (address);
    function token0() external view returns (address);
    function token1() external view returns (address);
}

interface IUniswapV2Factory {
    function getPair(address tokenA, address tokenB) external view returns (address pair);
}

interface IUniswapV2Callee {
    function uniswapV2Call(address sender, uint amount0, uint amount1, bytes calldata data) external;
    function pancakeCall(address sender, uint amount0, uint amount1, bytes calldata data) external;
}

/**
 * @title AdvancedFlashArbitrage
 * @notice نسخة مصلّحة:
 *  1) الـ factories صارت "موثّقة" (whitelist) من طرف الـ owner فقط بدل ما تكون
 *     باراميتر حر بالـ calldata — قبل هيك كان ممكن حدا يمرر factory وهمي
 *     يرجع getPair() = نفس الـ pair المزيّف، فيصير التحقق دائري وما بيحمي شي.
 *  2) إضافة تحقق إن borrowToken/borrowAmount فعلاً مطابقين لناتج swap على pair1،
 *     وإن repayToken هو فعلاً أحد توكني pair1 (وإلا العملية بترجع revert بدري
 *     بدل ما تعتمد فقط على فشل ضمني بمعادلة K تبع Uniswap).
 */
contract AdvancedFlashArbitrage is Ownable, ReentrancyGuard, IUniswapV2Callee {
    using SafeERC20 for IERC20;

    error Unprofitable();
    error TransferFailed();
    error Unauthorized();
    error InvalidPair();
    error InvalidAmounts();
    error Expired();

    event ArbitrageExecuted(address indexed profitToken, uint256 profit);
    event TrustedFactoryUpdated(address indexed factory, bool trusted);

    // العنوان الوحيد المسموح له بأن يكون "مقرض" أثناء التنفيذ الحالي
    address private _expectedPair1;

    // factories موثّقة فقط (يضيفها/يشيلها الـ owner) — هاد اللي بيمنع
    // تمرير factory وهمي مع الـ params لتزوير التحقق من الـ pair
    mapping(address => bool) public trustedFactories;

    constructor(address[] memory initialFactories) Ownable(msg.sender) {
        for (uint256 i = 0; i < initialFactories.length; i++) {
            trustedFactories[initialFactories[i]] = true;
            emit TrustedFactoryUpdated(initialFactories[i], true);
        }
    }

    function setTrustedFactory(address factory, bool trusted) external onlyOwner {
        trustedFactories[factory] = trusted;
        emit TrustedFactoryUpdated(factory, trusted);
    }

    struct ArbParams {
        address pair1;
        address pair2;
        address factory1;        // factory الخاص بـ pair1 (لازم يكون موثّق مسبقاً)
        address factory2;        // factory الخاص بـ pair2 (لازم يكون موثّق مسبقاً)
        address borrowToken;
        uint256 borrowAmount;
        uint256 amount0OutPair2;
        uint256 amount1OutPair2;
        address repayToken;
        uint256 repayAmount;
        address profitToken;
        uint256 minProfit;
        uint256 deadline;        // timestamp؛ 0 = بدون تحقق من الوقت
    }

    /**
     * @notice دالة البدء التي يتصل بها البوت الخاص بك
     */
    function executeFlashArbitrage(
        ArbParams calldata params,
        uint256 amount0OutPair1,
        uint256 amount1OutPair1
    ) external onlyOwner nonReentrant {
        if (params.deadline != 0 && block.timestamp > params.deadline) revert Expired();

        // لازم يكون فيه اتجاه اقتراض واحد وواضح فقط
        if ((amount0OutPair1 == 0) == (amount1OutPair1 == 0)) revert InvalidAmounts();

        // التحقق من أن pair1 و pair2 مسجّلان فعلاً في factory موثّق (وليس أي
        // factory يمرره المتصل بحرية)، ويرجّع توكنات pair1 لإعادة استخدامها
        (address token0Pair1, address token1Pair1) = _validatePair(params.pair1, params.factory1);
        _validatePair(params.pair2, params.factory2);

        // تأكيد إن التوكن/الكمية المطلوب اقتراضها مطابقة فعلاً لناتج swap على pair1
        if (amount0OutPair1 > 0) {
            if (params.borrowToken != token0Pair1 || params.borrowAmount != amount0OutPair1) revert InvalidAmounts();
        } else {
            if (params.borrowToken != token1Pair1 || params.borrowAmount != amount1OutPair1) revert InvalidAmounts();
        }

        // تأكيد إن توكن السداد هو فعلاً أحد توكني pair1 (وإلا Uniswap ما رح
        // يتعرف على السداد أصلاً وبترجع revert لاحقاً بمعادلة K)
        if (params.repayToken != token0Pair1 && params.repayToken != token1Pair1) revert InvalidPair();

        // ⚡ تحسين: استخدام Yul لـ balanceOf لتوفير الغاز
        // ⚠️ ملاحظة: ما فيك توصل لحقل بنية (params.profitToken) مباشرة جوه assembly —
        // لازم تسحبه لمتغير عادي أولاً (قيمة بسيطة بيقدر الـ Yul يشوفها بالاسم).
        address profitTokenForBalance = params.profitToken;
        uint256 balanceBefore;
        assembly {
            // balanceOf(address) selector = 0x70a08231
            let ptr := mload(0x40)
            mstore(ptr, shl(224, 0x70a08231))
            mstore(add(ptr, 0x04), address())
            let success := staticcall(gas(), profitTokenForBalance, ptr, 0x24, ptr, 0x20)
            if iszero(success) {
                revert(0, 0)
            }
            balanceBefore := mload(ptr)
        }

        bytes memory data = abi.encode(params, balanceBefore);

        // تسجيل العنوان المتوقع لمنع أي استدعاء عكسي من عنوان آخر أثناء نفس المعاملة
        _expectedPair1 = params.pair1;

        IUniswapV2Pair(params.pair1).swap(amount0OutPair1, amount1OutPair1, address(this), data);

        // تصفير الحالة بعد الانتهاء (توفير غاز عبر refund + نظافة الحالة)
        _expectedPair1 = address(0);
    }

    function uniswapV2Call(address sender, uint /*amount0*/, uint /*amount1*/, bytes calldata data) external override {
        _handleFlashSwap(sender, data);
    }

    function pancakeCall(address sender, uint /*amount0*/, uint /*amount1*/, bytes calldata data) external override {
        _handleFlashSwap(sender, data);
    }

    function _handleFlashSwap(address sender, bytes calldata data) internal {
        // لازم يكون فيه تنفيذ جارٍ ومسجل مسبقاً (يمنع استدعاء مباشر خارجي للدالة)
        if (msg.sender != _expectedPair1 || _expectedPair1 == address(0)) revert Unauthorized();
        if (sender != address(this)) revert Unauthorized();

        (ArbParams memory params, uint256 balanceBefore) = abi.decode(data, (ArbParams, uint256));

        // تأكيد إضافي إن الباراميترات المفكوكة تطابق العنوان المتصل فعلاً
        if (params.pair1 != msg.sender) revert Unauthorized();

        // ⚡ تحسين: استخدام Yul لـ transfer مع فحص الإرجاع لتوفير الغاز
        // ⚠️ نفس الملاحظة: حقول البنية (params.pair2 / params.borrowAmount /
        // params.borrowToken) لازم تُسحب لمتغيرات عادية قبل استخدامها بالـ assembly.
        address pair2ForTransfer = params.pair2;
        uint256 borrowAmountForTransfer = params.borrowAmount;
        address borrowTokenForTransfer = params.borrowToken;
        assembly {
            // transfer(address,uint256) selector = 0xa9059cbb
            let ptr := mload(0x40)
            mstore(ptr, shl(224, 0xa9059cbb))
            mstore(add(ptr, 0x04), pair2ForTransfer)
            mstore(add(ptr, 0x24), borrowAmountForTransfer)
            let success := call(gas(), borrowTokenForTransfer, 0, ptr, 0x44, ptr, 0x20)
            if iszero(success) {
                revert(0, 0)
            }
            // تحقّق من قيمة الإرجاع فقط إذا رجّع الكول بيانات فعلاً (بعض
            // التوكنات متل USDT ما بترجع أي شي مع نجاح العملية)
            if returndatasize() {
                if iszero(mload(ptr)) {
                    revert(0, 0)
                }
            }
        }

        IUniswapV2Pair(params.pair2).swap(
            params.amount0OutPair2,
            params.amount1OutPair2,
            address(this),
            new bytes(0)
        );

        IERC20(params.repayToken).safeTransfer(params.pair1, params.repayAmount);

        uint256 balanceAfter = IERC20(params.profitToken).balanceOf(address(this));

        if (balanceAfter <= balanceBefore) revert Unprofitable();

        uint256 profit = balanceAfter - balanceBefore;

        if (profit < params.minProfit) revert Unprofitable();

        emit ArbitrageExecuted(params.profitToken, profit);
    }

    /**
     * @dev يتحقق أن `pair` هو فعلاً الـ pair المسجل في `factory` الموثّق لنفس
     *      التوكنين اللذين يعلن أنه يحتفظ بهما، ويرجّع التوكنين لإعادة استخدامهم.
     *      يمنع تمرير عقد خبيث كـ pair1/pair2 أو factory وهمي.
     */
    function _validatePair(address pair, address factory) internal view returns (address token0, address token1) {
        if (pair == address(0) || factory == address(0)) revert InvalidPair();
        if (!trustedFactories[factory]) revert InvalidPair();

        token0 = IUniswapV2Pair(pair).token0();
        token1 = IUniswapV2Pair(pair).token1();

        address registered = IUniswapV2Factory(factory).getPair(token0, token1);
        if (registered != pair) revert InvalidPair();
    }

    function withdrawToken(address _token) external onlyOwner {
        uint256 balance = IERC20(_token).balanceOf(address(this));
        if (balance > 0) {
            IERC20(_token).safeTransfer(owner(), balance);
        }
    }

    function withdrawNative() external onlyOwner {
        uint256 balance = address(this).balance;
        if (balance > 0) {
            (bool success, ) = payable(owner()).call{value: balance}("");
            if (!success) revert TransferFailed();
        }
    }

    receive() external payable {}
}
