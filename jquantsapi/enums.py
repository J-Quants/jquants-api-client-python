from enum import Enum


class MARKET_API_SECTIONS(str, Enum):
    """
    values of sections for market api

    ref. (en) https://jpx.gitbook.io/j-quants-api-en/api-reference/markets-api/section-code
    ref. (ja) https://jpx.gitbook.io/j-quants-api/api-reference/market-api/sector_name
    """

    TSE1st = "TSE1st"
    TSE2nd = "TSE2nd"
    TSEMothers = "TSEMothers"
    TSEJASDAQ = "TSEJASDAQ"
    TSEPrime = "TSEPrime"
    TSEStandard = "TSEStandard"
    TSEGrowth = "TSEGrowth"
    TokyoNagoya = "TokyoNagoya"
