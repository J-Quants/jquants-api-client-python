# ref. ja https://jpx.gitbook.io/j-quants-ja/api-reference/listed_info
# ref. en https://jpx.gitbook.io/j-quants-en/api-reference/listed_info
LISTED_INFO_COLUMNS = [
    "Date",
    "Code",
    "CompanyName",
    "CompanyNameEnglish",
    "Sector17Code",
    "Sector17CodeName",
    "Sector33Code",
    "Sector33CodeName",
    "ScaleCategory",
    "MarketCode",
    "MarketCodeName",
]

LISTED_INFO_STANDARD_PREMIUM_COLUMNS = [
    "Date",
    "Code",
    "CompanyName",
    "CompanyNameEnglish",
    "Sector17Code",
    "Sector17CodeName",
    "Sector33Code",
    "Sector33CodeName",
    "ScaleCategory",
    "MarketCode",
    "MarketCodeName",
    "MarginCode",
    "MarginCodeName",
]

# ref. ja https://jpx.gitbook.io/j-quants-ja/api-reference/listed_info/sector17code
# ref. en https://jpx.gitbook.io/j-quants-en/api-reference/listed_info/sector17code
SECTOR_17_COLUMNS = ["Sector17Code", "Sector17CodeName", "Sector17CodeNameEnglish"]
SECTOR_17_DATA = [
    ("1", "食品", "FOODS"),
    ("2", "エネルギー資源", "ENERGY RESOURCES"),
    ("3", "建設・資材", "CONSTRUCTION & MATERIALS"),
    ("4", "素材・化学", "RAW MATERIALS & CHEMICALS"),
    ("5", "医薬品", "PHARMACEUTICAL"),
    ("6", "自動車・輸送機", "AUTOMOBILES & TRANSPORTATION EQUIPMEN"),
    ("7", "鉄鋼・非鉄", "STEEL & NONFERROUS METALS"),
    ("8", "機械", "MACHINERY"),
    ("9", "電機・精密", "ELECTRIC APPLIANCES & PRECISION INSTRUMENTS"),
    ("10", "情報通信・サービスその他", "IT & SERVICES, OTHERS "),
    ("11", "電気・ガス", "ELECTRIC POWER & GAS"),
    ("12", "運輸・物流", "TRANSPORTATION & LOGISTICS"),
    ("13", "商社・卸売", "COMMERCIAL & WHOLESALE TRADE"),
    ("14", "小売", "RETAIL TRADE"),
    ("15", "銀行", "BANKS"),
    ("16", "金融（除く銀行）", "FINANCIALS (EX BANKS) "),
    ("17", "不動産", "REAL ESTATE"),
    ("99", "その他", "OTHER"),
]

# ref. ja https://jpx.gitbook.io/j-quants-ja/api-reference/listed_info/sector33code
# ref. en https://jpx.gitbook.io/j-quants-en/api-reference/listed_info/sector33code
# ref. 33-17 mapping https://www.jpx.co.jp/markets/indices/line-up/files/fac_13_sector.pdf
SECTOR_33_COLUMNS = [
    "Sector33Code",
    "Sector33CodeName",
    "Sector33CodeNameEnglish",
    "Sector17Code",
]
SECTOR_33_DATA = [
    ("0050", "水産・農林業", "Fishery, Agriculture & Forestry", "1"),
    ("1050", "鉱業", "Mining", "2"),
    ("2050", "建設業", "Construction", "3"),
    ("3050", "食料品", "Foods", "1"),
    ("3100", "繊維製品", "Textiles & Apparels", "4"),
    ("3150", "パルプ・紙", "Pulp & Paper", "4"),
    ("3200", "化学", "Chemicals", "4"),
    ("3250", "医薬品", "Pharmaceutical", "5"),
    ("3300", "石油･石炭製品", "Oil & Coal Products", "2"),
    ("3350", "ゴム製品", "Rubber Products", "6"),
    ("3400", "ガラス･土石製品", "Glass & Ceramics Products", "3"),
    ("3450", "鉄鋼", "Iron & Steel", "7"),
    ("3500", "非鉄金属", "Nonferrous Metals", "7"),
    ("3550", "金属製品", "Metal Products", "3"),
    ("3600", "機械", "Machinery", "8"),
    ("3650", "電気機器", "Electric Appliances", "9"),
    ("3700", "輸送用機器", "Transportation Equipment", "6"),
    ("3750", "精密機器", "Precision Instruments", "9"),
    ("3800", "その他製品", "Other Products", "10"),
    ("4050", "電気･ガス業", "Electric Power & Gas", "11"),
    ("5050", "陸運業", "Land Transportation", "12"),
    ("5100", "海運業", "Marine Transportation", "12"),
    ("5150", "空運業", "Air Transportation", "12"),
    ("5200", "倉庫･運輸関連業", "Warehousing & Harbor Transportation Services", "12"),
    ("5250", "情報･通信業", "Information & Communication", "10"),
    ("6050", "卸売業", "Wholesale Trade", "13"),
    ("6100", "小売業", "Retail Trade", "14"),
    ("7050", "銀行業", "Banks", "15"),
    ("7100", "証券､商品先物取引業", "Securities & Commodity Futures", "16"),
    ("7150", "保険業", "Insurance", "16"),
    ("7200", "その他金融業", "Other Financing Business", "16"),
    ("8050", "不動産業", "Real Estate", "17"),
    ("9050", "サービス業", "Services", "10"),
    ("9999", "その他", "Other", "99"),
]

# ref. ja https://jpx.gitbook.io/j-quants-ja/api-reference/daily_quotes
# ref. en https://jpx.gitbook.io/j-quants-en/api-reference/daily_quotes
PRICES_DAILY_QUOTES_COLUMNS = [
    "Date",
    "Code",
    "Open",
    "High",
    "Low",
    "Close",
    "UpperLimit",
    "LowerLimit",
    "Volume",
    "TurnoverValue",
    "AdjustmentFactor",
    "AdjustmentOpen",
    "AdjustmentHigh",
    "AdjustmentLow",
    "AdjustmentClose",
    "AdjustmentVolume",
]

PRICES_DAILY_QUOTES_PREMIUM_COLUMNS = [
    "Date",
    "Code",
    "Open",
    "High",
    "Low",
    "Close",
    "UpperLimit",
    "LowerLimit",
    "Volume",
    "TurnoverValue",
    "AdjustmentFactor",
    "AdjustmentOpen",
    "AdjustmentHigh",
    "AdjustmentLow",
    "AdjustmentClose",
    "AdjustmentVolume",
    "MorningOpen",
    "MorningHigh",
    "MorningLow",
    "MorningClose",
    "MorningUpperLimit",
    "MorningLowerLimit",
    "MorningVolume",
    "MorningTurnoverValue",
    "MorningAdjustmentOpen",
    "MorningAdjustmentHigh",
    "MorningAdjustmentLow",
    "MorningAdjustmentClose",
    "MorningAdjustmentVolume",
    "AfternoonOpen",
    "AfternoonHigh",
    "AfternoonLow",
    "AfternoonClose",
    "AfternoonUpperLimit",
    "AfternoonLowerLimit",
    "AfternoonVolume",
    "AfternoonTurnoverValue",
    "AfternoonAdjustmentOpen",
    "AfternoonAdjustmentHigh",
    "AfternoonAdjustmentLow",
    "AfternoonAdjustmentClose",
    "AfternoonAdjustmentVolume",
]

# ref. ja https://jpx.gitbook.io/j-quants-ja/api-reference/indices
# ref. en https://jpx.gitbook.io/j-quants-en/api-reference/indices
INDICES_COLUMNS = [
    "Date",
    "Code",
    "Open",
    "High",
    "Low",
    "Close",
]

# ref. ja https://jpx.gitbook.io/j-quants-ja/api-reference/topix
# ref. en https://jpx.gitbook.io/j-quants-en/api-reference/topix
INDICES_TOPIX_COLUMNS = [
    "Date",
    "Open",
    "High",
    "Low",
    "Close",
]

# ref. ja https://jpx.gitbook.io/j-quants-ja/api-reference/trades_spec
# ref. en https://jpx.gitbook.io/j-quants-en/api-reference/trades_spec
MARKETS_TRADES_SPEC = [
    "Section",
    "PublishedDate",
    "StartDate",
    "EndDate",
    "ProprietarySales",
    "ProprietaryPurchases",
    "ProprietaryTotal",
    "ProprietaryBalance",
    "BrokerageSales",
    "BrokeragePurchases",
    "BrokerageTotal",
    "BrokerageBalance",
    "TotalSales",
    "TotalPurchases",
    "TotalTotal",
    "TotalBalance",
    "IndividualsSales",
    "IndividualsPurchases",
    "IndividualsTotal",
    "IndividualsBalance",
    "ForeignersSales",
    "ForeignersPurchases",
    "ForeignersTotal",
    "ForeignersBalance",
    "SecuritiesCosSales",
    "SecuritiesCosPurchases",
    "SecuritiesCosTotal",
    "SecuritiesCosBalance",
    "InvestmentTrustsSales",
    "InvestmentTrustsPurchases",
    "InvestmentTrustsTotal",
    "InvestmentTrustsBalance",
    "BusinessCosSales",
    "BusinessCosPurchases",
    "BusinessCosTotal",
    "BusinessCosBalance",
    "OtherCosSales",
    "OtherCosPurchases",
    "OtherCosTotal",
    "OtherCosBalance",
    "InsuranceCosSales",
    "InsuranceCosPurchases",
    "InsuranceCosTotal",
    "InsuranceCosBalance",
    "CityBKsRegionalBKsEtcSales",
    "CityBKsRegionalBKsEtcPurchases",
    "CityBKsRegionalBKsEtcTotal",
    "CityBKsRegionalBKsEtcBalance",
    "TrustBanksSales",
    "TrustBanksPurchases",
    "TrustBanksTotal",
    "TrustBanksBalance",
    "OtherFinancialInstitutionsSales",
    "OtherFinancialInstitutionsPurchases",
    "OtherFinancialInstitutionsTotal",
    "OtherFinancialInstitutionsBalance",
]

# ref. ja https://jpx.gitbook.io/j-quants-ja/api-reference/weekly_margin_interest
# ref. en https://jpx.gitbook.io/j-quants-en/api-reference/weekly_margin_interest
MARKETS_WEEKLY_MARGIN_INTEREST = [
    "Date",
    "Code",
    "ShortMarginTradeVolume",
    "LongMarginTradeVolume",
    "ShortNegotiableMarginTradeVolume",
    "LongNegotiableMarginTradeVolume",
    "ShortStandardizedMarginTradeVolume",
    "LongStandardizedMarginTradeVolume",
    "IssueType",
]

# ref. ja https://jpx.gitbook.io/j-quants-ja/api-reference/listed_info/marketcode
# ref. en https://jpx.gitbook.io/j-quants-en/api-reference/listed_info/marketcode
MARKET_SEGMENT_COLUMNS = [
    "MarketCode",
    "MarketCodeName",
    "MarketCodeNameEnglish",
]
MARKET_SEGMENT_DATA = [
    ("0101", "東証一部", "1st Section"),
    ("0102", "東証二部", "2nd Section"),
    ("0104", "マザーズ", "Mothers"),
    ("0105", "TOKYO PRO MARKET", "TOKYO PRO MARKET"),
    ("0106", "JASDAQ スタンダード", "JASDAQ Standard"),
    ("0107", "JASDAQ グロース", "JASDAQ Growth"),
    ("0109", "その他", "Others"),
    ("0111", "プライム", "Prime"),
    ("0112", "スタンダード", "Standard"),
    ("0113", "グロース", "Growth"),
]

# ref ja https://jpx.gitbook.io/j-quants-ja/api-reference/statements
# ref en https://jpx.gitbook.io/j-quants-en/api-reference/statements
FINS_STATEMENTS_COLUMNS = [
    "DisclosedDate",
    "DisclosedTime",
    "LocalCode",
    "DisclosureNumber",
    "TypeOfDocument",
    "TypeOfCurrentPeriod",
    "CurrentPeriodStartDate",
    "CurrentPeriodEndDate",
    "CurrentFiscalYearStartDate",
    "CurrentFiscalYearEndDate",
    "NextFiscalYearStartDate",
    "NextFiscalYearEndDate",
    "NetSales",
    "OperatingProfit",
    "OrdinaryProfit",
    "Profit",
    "EarningsPerShare",
    "DilutedEarningsPerShare",
    "TotalAssets",
    "Equity",
    "EquityToAssetRatio",
    "BookValuePerShare",
    "CashFlowsFromOperatingActivities",
    "CashFlowsFromInvestingActivities",
    "CashFlowsFromFinancingActivities",
    "CashAndEquivalents",
    "ResultDividendPerShare1stQuarter",
    "ResultDividendPerShare2ndQuarter",
    "ResultDividendPerShare3rdQuarter",
    "ResultDividendPerShareFiscalYearEnd",
    "ResultDividendPerShareAnnual",
    "DistributionsPerUnit(REIT)",
    "ResultTotalDividendPaidAnnual",
    "ResultPayoutRatioAnnual",
    "ForecastDividendPerShare1stQuarter",
    "ForecastDividendPerShare2ndQuarter",
    "ForecastDividendPerShare3rdQuarter",
    "ForecastDividendPerShareFiscalYearEnd",
    "ForecastDividendPerShareAnnual",
    "ForecastDistributionsPerUnit(REIT)",
    "ForecastTotalDividendPaidAnnual",
    "ForecastPayoutRatioAnnual",
    "NextYearForecastDividendPerShare1stQuarter",
    "NextYearForecastDividendPerShare2ndQuarter",
    "NextYearForecastDividendPerShare3rdQuarter",
    "NextYearForecastDividendPerShareFiscalYearEnd",
    "NextYearForecastDividendPerShareAnnual",
    "NextYearForecastDistributionsPerUnit(REIT)",
    "NextYearForecastPayoutRatioAnnual",
    "ForecastNetSales2ndQuarter",
    "ForecastOperatingProfit2ndQuarter",
    "ForecastOrdinaryProfit2ndQuarter",
    "ForecastProfit2ndQuarter",
    "ForecastEarningsPerShare2ndQuarter",
    "NextYearForecastNetSales2ndQuarter",
    "NextYearForecastOperatingProfit2ndQuarter",
    "NextYearForecastOrdinaryProfit2ndQuarter",
    "NextYearForecastProfit2ndQuarter",
    "NextYearForecastEarningsPerShare2ndQuarter",
    "ForecastNetSales",
    "ForecastOperatingProfit",
    "ForecastOrdinaryProfit",
    "ForecastProfit",
    "ForecastEarningsPerShare",
    "NextYearForecastNetSales",
    "NextYearForecastOperatingProfit",
    "NextYearForecastOrdinaryProfit",
    "NextYearForecastProfit",
    "NextYearForecastEarningsPerShare",
    "MaterialChangesInSubsidiaries",
    "SignificantChangesInTheScopeOfConsolidation",
    "ChangesBasedOnRevisionsOfAccountingStandard",
    "ChangesOtherThanOnesBasedOnRevisionsOfAccountingStandard",
    "ChangesInAccountingEstimates",
    "RetrospectiveRestatement",
    "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock",
    "NumberOfTreasuryStockAtTheEndOfFiscalYear",
    "AverageNumberOfShares",
    "NonConsolidatedNetSales",
    "NonConsolidatedOperatingProfit",
    "NonConsolidatedOrdinaryProfit",
    "NonConsolidatedProfit",
    "NonConsolidatedEarningsPerShare",
    "NonConsolidatedTotalAssets",
    "NonConsolidatedEquity",
    "NonConsolidatedEquityToAssetRatio",
    "NonConsolidatedBookValuePerShare",
    "ForecastNonConsolidatedNetSales2ndQuarter",
    "ForecastNonConsolidatedOperatingProfit2ndQuarter",
    "ForecastNonConsolidatedOrdinaryProfit2ndQuarter",
    "ForecastNonConsolidatedProfit2ndQuarter",
    "ForecastNonConsolidatedEarningsPerShare2ndQuarter",
    "NextYearForecastNonConsolidatedNetSales2ndQuarter",
    "NextYearForecastNonConsolidatedOperatingProfit2ndQuarter",
    "NextYearForecastNonConsolidatedOrdinaryProfit2ndQuarter",
    "NextYearForecastNonConsolidatedProfit2ndQuarter",
    "NextYearForecastNonConsolidatedEarningsPerShare2ndQuarter",
    "ForecastNonConsolidatedNetSales",
    "ForecastNonConsolidatedOperatingProfit",
    "ForecastNonConsolidatedOrdinaryProfit",
    "ForecastNonConsolidatedProfit",
    "ForecastNonConsolidatedEarningsPerShare",
    "NextYearForecastNonConsolidatedNetSales",
    "NextYearForecastNonConsolidatedOperatingProfit",
    "NextYearForecastNonConsolidatedOrdinaryProfit",
    "NextYearForecastNonConsolidatedProfit",
    "NextYearForecastNonConsolidatedEarningsPerShare",
]

# ref. ja https://jpx.gitbook.io/j-quants-ja/api-reference/announcement
# ref. en https://jpx.gitbook.io/j-quants-en/api-reference/announcement
FINS_ANNOUNCEMENT_COLUMNS = [
    "Code",
    "Date",
    "CompanyName",
    "FiscalYear",
    "SectorName",
    "FiscalQuarter",
    "Section",
]

# ref. ja https://jpx.gitbook.io/j-quants-ja/api-reference/short_selling
# ref. en https://jpx.gitbook.io/j-quants-en/api-reference/short_selling
MARKET_SHORT_SELLING_COLUMNS = [
    "Date",
    "Sector33Code",
    "SellingExcludingShortSellingTurnoverValue",
    "ShortSellingWithRestrictionsTurnoverValue",
    "ShortSellingWithoutRestrictionsTurnoverValue",
]
# ref. ja https://jpx.gitbook.io/j-quants-ja/api-reference/index_option
# ref. en https://jpx.gitbook.io/j-quants-en/api-reference/index_option
OPTION_INDEX_OPTION_COLUMNS = [
    "Date",
    "Code",
    "WholeDayOpen",
    "WholeDayHigh",
    "WholeDayLow",
    "WholeDayClose",
    "NightSessionOpen",
    "NightSessionHigh",
    "NightSessionLow",
    "NightSessionClose",
    "DaySessionOpen",
    "DaySessionHigh",
    "DaySessionLow",
    "DaySessionClose",
    "Volume",
    "OpenInterest",
    "TurnoverValue",
    "ContractMonth",
    "StrikePrice",
    "Volume(OnlyAuction)",
    "EmergencyMarginTriggerDivision",
    "PutCallDivision",
    "LastTradingDay",
    "SpecialQuotationDay",
    "SettlementPrice",
    "TheoreticalPrice",
    "BaseVolatility",
    "UnderlyingPrice",
    "ImpliedVolatility",
    "InterestRate",
]

MARKETS_BREAKDOWN_COLUMNS = [
    "Date",
    "Code",
    "LongSellValue",
    "ShortSellWithoutMarginValue",
    "MarginSellNewValue",
    "MarginSellCloseValue",
    "LongBuyValue",
    "MarginBuyNewValue",
    "MarginBuyCloseValue",
    "LongSellVolume",
    "ShortSellWithoutMarginVolume",
    "MarginSellNewVolume",
    "MarginSellCloseVolume",
    "LongBuyVolume",
    "MarginBuyNewVolume",
    "MarginBuyCloseVolume",
]

FINS_DIVIDEND_COLUMNS = [
    "AnnouncementDate",
    "AnnouncementTime",
    "Code",
    "ReferenceNumber",
    "StatusCode",
    "BoardMeetingDate",
    "InterimFinalCode",
    "ForecastResultCode",
    "InterimFinalTerm",
    "GrossDividendRate",
    "RecordDate",
    "ExDate",
    "ActualRecordDate",
    "PayableDate",
    "CAReferenceNumber",
    "DistributionAmount",
    "RetainedEarnings",
    "DeemedDividend",
    "DeemedCapitalGains",
    "NetAssetDecreaseRatio",
    "CommemorativeSpecialCode",
    "CommemorativeDividendRate",
    "SpecialDividendRate",
]

PRICES_PRICES_AM_COLUMNS = [
    "Date",
    "Code",
    "MorningOpen",
    "MorningHigh",
    "MorningLow",
    "MorningClose",
    "MorningVolume",
    "MorningTurnoverValue",
]

MARKETS_TRADING_CALENDAR = [
    "Date",
    "HolidayDivision",
]

FINS_FS_DETAILS_COLUMNS = [
    "DisclosedDate",
    "DisclosedTime",
    "LocalCode",
    "DisclosureNumber",
    "TypeOfDocument",
]

DERIVATIVES_FUTURES_COLUMNS = [
    "Date",
    "Code",
    "DerivativesProductCategory",
    "WholeDayOpen",
    "WholeDayHigh",
    "WholeDayLow",
    "WholeDayClose",
    "MorningSessionOpen",
    "MorningSessionHigh",
    "MorningSessionLow",
    "MorningSessionClose",
    "NightSessionOpen",
    "NightSessionHigh",
    "NightSessionLow",
    "NightSessionClose",
    "DaySessionOpen",
    "DaySessionHigh",
    "DaySessionLow",
    "DaySessionClose",
    "Volume",
    "OpenInterest",
    "TurnoverValue",
    "ContractMonth",
    "Volume(OnlyAuction)",
    "EmergencyMarginTriggerDivision",
    "LastTradingDay",
    "SpecialQuotationDay",
    "SettlementPrice",
    "CentralContractMonthFlag",
]

DERIVATIVES_OPTIONS_COLUMNS = [
    "Date",
    "Code",
    "DerivativesProductCategory",
    "UnderlyingSSO",
    "WholeDayOpen",
    "WholeDayHigh",
    "WholeDayLow",
    "WholeDayClose",
    "MorningSessionOpen",
    "MorningSessionHigh",
    "MorningSessionLow",
    "MorningSessionClose",
    "NightSessionOpen",
    "NightSessionHigh",
    "NightSessionLow",
    "NightSessionClose",
    "DaySessionOpen",
    "DaySessionHigh",
    "DaySessionLow",
    "DaySessionClose",
    "Volume",
    "OpenInterest",
    "TurnoverValue",
    "ContractMonth",
    "StrikePrice",
    "Volume(OnlyAuction)",
    "EmergencyMarginTriggerDivision",
    "PutCallDivision",
    "LastTradingDay",
    "SpecialQuotationDay",
    "SettlementPrice",
    "TheoreticalPrice",
    "BaseVolatility",
    "UnderlyingPrice",
    "ImpliedVolatility",
    "InterestRate",
    "CentralContractMonthFlag",
]

SHORT_SELLING_POSITIONS_COLUMNS = [
    "DisclosedDate",
    "CalculatedDate",
    "Code",
    "ShortSellerName",
    "ShortSellerAddress",
    "DiscretionaryInvestmentContractorName",
    "DiscretionaryInvestmentContractorAddress",
    "InvestmentFundName",
    "ShortPositionsToSharesOutstandingRatio",
    "ShortPositionsInSharesNumber",
    "ShortPositionsInTradingUnitsNumber",
    "CalculationInPreviousReportingDate",
    "ShortPositionsInPreviousReportingRatio",
    "Notes",
]
