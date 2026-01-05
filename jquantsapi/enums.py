from enum import Enum


class MARKET_API_SECTIONS(str, Enum):
    """
    values of sections for market api

    ref. (ja) https://jpx-jquants.com/ja/spec/eq-investor-types/section
    ref. (en) https://jpx-jquants.com/en/spec/eq-investor-types/section
    """

    TSE1st = "TSE1st"
    TSE2nd = "TSE2nd"
    TSEMothers = "TSEMothers"
    TSEJASDAQ = "TSEJASDAQ"
    TSEPrime = "TSEPrime"
    TSEStandard = "TSEStandard"
    TSEGrowth = "TSEGrowth"
    TokyoNagoya = "TokyoNagoya"


class BulkEndpoint(str, Enum):
    """
    Bulk APIで指定可能なエンドポイント
    """

    # 上場銘柄一覧API
    EQ_MASTER = "/equities/master"
    # 株価四本値API
    EQ_BARS_DAILY = "/equities/bars/daily"
    # 分足API
    EQ_BARS_MINUTE = "/equities/bars/minute"
    # 投資部門別売買状況API
    EQ_INVESTOR_TYPES = "/equities/investor-types"
    # TickデータAPI
    EQ_TRADES = "/equities/trades"

    # 財務情報API
    FIN_SUMMARY = "/fins/summary"
    # 財務諸表API
    FIN_DETAILS = "/fins/details"
    # 配当金情報API
    FIN_DIVIDEND = "/fins/dividend"

    # 業種別空売り比率API
    MKT_SHORT_RATIO = "/markets/short-ratio"
    # 空売り残高報告API
    MKT_SHORT_SALE_REPORT = "/markets/short-sale-report"
    # 信用取引週末残高API
    MKT_MARGIN_INTEREST = "/markets/margin-interest"
    # 日々公表信用取引残高API
    MKT_MARGIN_ALERT = "/markets/margin-alert"
    # 売買内訳データAPI
    MKT_BREAKDOWN = "/markets/breakdown"

    # 指数四本値API
    IDX_BARS_DAILY = "/indices/bars/daily"
    # TOPIX指数四本値API
    IDX_BARS_DAILY_TOPIX = "/indices/bars/daily/topix"

    # 先物四本値API
    DRV_BARS_DAILY_FUT = "/derivatives/bars/daily/futures"
    # オプション四本値API
    DRV_BARS_DAILY_OPT = "/derivatives/bars/daily/options"
    # 日経225オプション四本値API
    DRV_BARS_DAILY_OPT_225 = "/derivatives/bars/daily/options/225"
