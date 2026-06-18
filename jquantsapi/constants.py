# ============================================================================
# 共通データ
# ============================================================================

# ref. ja https://jpx-jquants.com/ja/spec/eq-master/sector17code
# ref. en https://jpx-jquants.com/en/spec/eq-master/sector17code
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

# ref. ja https://jpx-jquants.com/ja/spec/eq-master/sector33code
# ref. en https://jpx-jquants.com/en/spec/eq-master/sector33code
# ref. 33-17 mapping https://www.jpx.co.jp/markets/indices/line-up/files/fac_13_sector.pdf
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

# ref. ja https://jpx-jquants.com/ja/spec/eq-master/marketcode
# ref. en https://jpx-jquants.com/en/spec/eq-master/marketcode
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


# ============================================================================
# API用カラム定義
# ============================================================================

# ref. ja https://jpx-jquants.com/ja/spec/eq-master/sector17code
# ref. en https://jpx-jquants.com/en/spec/eq-master/sector17code
SECTOR_17_COLUMNS_V2 = ["S17", "S17Nm", "S17NmEn"]

# ref. ja https://jpx-jquants.com/ja/spec/eq-master/sector33code
# ref. en https://jpx-jquants.com/en/spec/eq-master/sector33code
SECTOR_33_COLUMNS_V2 = ["S33", "S33Nm", "S33NmEn", "S17"]

# ref. ja https://jpx-jquants.com/ja/spec/eq-master/marketcode
# ref. en https://jpx-jquants.com/en/spec/eq-master/marketcode
MARKET_SEGMENT_COLUMNS_V2 = ["Mkt", "MktNm", "MktNmEn"]

# ref. ja https://jpx-jquants.com/ja/spec/eq-master
# ref. en https://jpx-jquants.com/en/spec/eq-master
EQ_MASTER_COLUMNS_V2 = [
    "Date",
    "Code",
    "CoName",
    "CoNameEn",
    "S17",
    "S17Nm",
    "S33",
    "S33Nm",
    "ScaleCat",
    "Mkt",
    "MktNm",
    "Mrgn",
    "MrgnNm",
]

# ref. ja https://jpx-jquants.com/ja/spec/eq-bars-daily
# ref. en https://jpx-jquants.com/en/spec/eq-bars-daily
EQ_BARS_DAILY_COLUMNS_V2 = [
    "Date",
    "Code",
    "O",
    "H",
    "L",
    "C",
    "UL",
    "LL",
    "Vo",
    "Va",
    "AdjFactor",
    "AdjO",
    "AdjH",
    "AdjL",
    "AdjC",
    "AdjVo",
    "MO",
    "MH",
    "ML",
    "MC",
    "MUL",
    "MLL",
    "MVo",
    "MVa",
    "MAdjO",
    "MAdjH",
    "MAdjL",
    "MAdjC",
    "MAdjVo",
    "AO",
    "AH",
    "AL",
    "AC",
    "AUL",
    "ALL",
    "AVo",
    "AVa",
    "AAdjO",
    "AAdjH",
    "AAdjL",
    "AAdjC",
    "AAdjVo",
]

# ref. ja https://jpx-jquants.com/ja/spec/eq-bars-minute
# ref. en https://jpx-jquants.com/en/spec/eq-bars-minute
EQ_BARS_MINUTE_COLUMNS_V2 = [
    "Date",
    "Time",
    "Code",
    "O",
    "H",
    "L",
    "C",
    "Vo",
    "Va",
]

# ref. ja https://jpx-jquants.com/ja/spec/eq-bars-daily-am
# ref. en https://jpx-jquants.com/en/spec/eq-bars-daily-am
PRICES_PRICES_AM_COLUMNS_V2 = [
    "Date",
    "Code",
    "MO",
    "MH",
    "ML",
    "MC",
    "MVo",
    "MVa",
]

# ref. ja https://jpx-jquants.com/ja/spec/idx-bars-daily
# ref. en https://jpx-jquants.com/en/spec/idx-bars-daily
INDICES_COLUMNS_V2 = [
    "Date",
    "Code",
    "O",
    "H",
    "L",
    "C",
]

# ref. ja https://jpx-jquants.com/ja/spec/idx-bars-daily-topix
# ref. en https://jpx-jquants.com/en/spec/idx-bars-daily-topix
INDICES_TOPIX_COLUMNS_V2 = [
    "Date",
    "O",
    "H",
    "L",
    "C",
]

# ref. ja https://jpx-jquants.com/ja/spec/eq-investor-types
# ref. en https://jpx-jquants.com/en/spec/eq-investor-types
EQ_INVESTOR_TYPES_COLUMNS_V2 = [
    "PubDate",
    "StDate",
    "EnDate",
    "Section",
    "PropSell",
    "PropBuy",
    "PropTot",
    "PropBal",
    "BrkSell",
    "BrkBuy",
    "BrkTot",
    "BrkBal",
    "TotSell",
    "TotBuy",
    "TotTot",
    "TotBal",
    "IndSell",
    "IndBuy",
    "IndTot",
    "IndBal",
    "FrgnSell",
    "FrgnBuy",
    "FrgnTot",
    "FrgnBal",
    "SecCoSell",
    "SecCoBuy",
    "SecCoTot",
    "SecCoBal",
    "InvTrSell",
    "InvTrBuy",
    "InvTrTot",
    "InvTrBal",
    "BusCoSell",
    "BusCoBuy",
    "BusCoTot",
    "BusCoBal",
    "OthCoSell",
    "OthCoBuy",
    "OthCoTot",
    "OthCoBal",
    "InsCoSell",
    "InsCoBuy",
    "InsCoTot",
    "InsCoBal",
    "BankSell",
    "BankBuy",
    "BankTot",
    "BankBal",
    "TrstBnkSell",
    "TrstBnkBuy",
    "TrstBnkTot",
    "TrstBnkBal",
    "OthFinSell",
    "OthFinBuy",
    "OthFinTot",
    "OthFinBal",
]

# ref. ja https://jpx-jquants.com/ja/spec/mkt-margin-int
# ref. en https://jpx-jquants.com/en/spec/mkt-margin-int
MARKETS_WEEKLY_MARGIN_INTEREST_COLUMNS_V2 = [
    "Date",
    "Code",
    "ShrtVol",
    "LongVol",
    "ShrtNegVol",
    "LongNegVol",
    "ShrtStdVol",
    "LongStdVol",
    "IssType",
]

# ref. ja https://jpx-jquants.com/ja/spec/mkt-short-ratio
# ref. en https://jpx-jquants.com/en/spec/mkt-short-ratio
MKT_SHORT_RATIO_COLUMNS_V2 = [
    "Date",
    "S33",
    "SellExShortVa",
    "ShrtWithResVa",
    "ShrtNoResVa",
]

# ref. ja https://jpx-jquants.com/ja/spec/mkt-breakdown
# ref. en https://jpx-jquants.com/en/spec/mkt-breakdown
MKT_BREAKDOWN_COLUMNS_V2 = [
    "Date",
    "Code",
    "LongSellVa",
    "ShrtNoMrgnVa",
    "MrgnSellNewVa",
    "MrgnSellCloseVa",
    "LongBuyVa",
    "MrgnBuyNewVa",
    "MrgnBuyCloseVa",
    "LongSellVo",
    "ShrtNoMrgnVo",
    "MrgnSellNewVo",
    "MrgnSellCloseVo",
    "LongBuyVo",
    "MrgnBuyNewVo",
    "MrgnBuyCloseVo",
]

# ref. ja https://jpx-jquants.com/ja/spec/mkt-cal
# ref. en https://jpx-jquants.com/en/spec/mkt-cal
MARKETS_TRADING_CALENDAR_COLUMNS_V2 = [
    "Date",
    "HolDiv",
]

# ref. ja https://jpx-jquants.com/ja/spec/fin-summary
# ref. en https://jpx-jquants.com/en/spec/fin-summary
FIN_SUMMARY_COLUMNS_V2 = [
    "DiscDate",
    "DiscTime",
    "Code",
    "DiscNo",
    "DocType",
    "CurPerType",
    "CurPerSt",
    "CurPerEn",
    "CurFYSt",
    "CurFYEn",
    "NxtFYSt",
    "NxtFYEn",
    "Sales",
    "OP",
    "OdP",
    "NP",
    "EPS",
    "DEPS",
    "TA",
    "Eq",
    "EqAR",
    "BPS",
    "CFO",
    "CFI",
    "CFF",
    "CashEq",
    "Div1Q",
    "Div2Q",
    "Div3Q",
    "DivFY",
    "DivAnn",
    "DivUnit",
    "DivTotalAnn",
    "PayoutRatioAnn",
    "FDiv1Q",
    "FDiv2Q",
    "FDiv3Q",
    "FDivFY",
    "FDivAnn",
    "FDivUnit",
    "FDivTotalAnn",
    "FPayoutRatioAnn",
    "NxFDiv1Q",
    "NxFDiv2Q",
    "NxFDiv3Q",
    "NxFDivFY",
    "NxFDivAnn",
    "NxFDivUnit",
    "NxFPayoutRatioAnn",
    "FSales2Q",
    "FOP2Q",
    "FOdP2Q",
    "FNP2Q",
    "FEPS2Q",
    "NxFSales2Q",
    "NxFOP2Q",
    "NxFOdP2Q",
    "NxFNp2Q",
    "NxFEPS2Q",
    "FSales",
    "FOP",
    "FOdP",
    "FNP",
    "FEPS",
    "NxFSales",
    "NxFOP",
    "NxFOdP",
    "NxFNp",
    "NxFEPS",
    "MatChgSub",
    "SigChgInC",
    "ChgByASRev",
    "ChgNoASRev",
    "ChgAcEst",
    "RetroRst",
    "ShOutFY",
    "TrShFY",
    "AvgSh",
    "NCSales",
    "NCOP",
    "NCOdP",
    "NCNP",
    "NCEPS",
    "NCTA",
    "NCEq",
    "NCEqAR",
    "NCBPS",
    "FNCSales2Q",
    "FNCOP2Q",
    "FNCOdP2Q",
    "FNCNP2Q",
    "FNCEPS2Q",
    "NxFNCSales2Q",
    "NxFNCOP2Q",
    "NxFNCOdP2Q",
    "NxFNCNP2Q",
    "NxFNCEPS2Q",
    "FNCSales",
    "FNCOP",
    "FNCOdP",
    "FNCNP",
    "FNCEPS",
    "NxFNCSales",
    "NxFNCOP",
    "NxFNCOdP",
    "NxFNCNP",
    "NxFNCEPS",
]

# ref. ja https://jpx-jquants.com/ja/spec/fin-details
# ref. en https://jpx-jquants.com/en/spec/fin-details
FINS_FS_DETAILS_COLUMNS_V2 = [
    "DiscDate",
    "DiscTime",
    "Code",
    "DiscNo",
    "DocType",
    "FS",
]

# ref. ja https://jpx-jquants.com/ja/spec/fin-dividend
# ref. en https://jpx-jquants.com/en/spec/fin-dividend
FINS_DIVIDEND_COLUMNS_V2 = [
    "PubDate",
    "PubTime",
    "Code",
    "RefNo",
    "StatCode",
    "BoardDate",
    "IFCode",
    "FRCode",
    "IFTerm",
    "DivRate",
    "RecDate",
    "ExDate",
    "ActRecDate",
    "PayDate",
    "CARefNo",
    "DistAmt",
    "RetEarn",
    "DeemDiv",
    "DeemCapGains",
    "NetAssetDecRatio",
    "CommSpecCode",
    "CommDivRate",
    "SpecDivRate",
]

# ref. ja https://jpx-jquants.com/ja/spec/eq-earnings-cal
# ref. en https://jpx-jquants.com/en/spec/eq-earnings-cal
FINS_ANNOUNCEMENT_COLUMNS_V2 = [
    "Date",
    "Code",
    "CoName",
    "FY",
    "SectorNm",
    "FQ",
    "Section",
]

# ref. ja https://jpx-jquants.com/ja/spec/drv-bars-daily-fut
# ref. en https://jpx-jquants.com/en/spec/drv-bars-daily-fut
DERIVATIVES_FUTURES_COLUMNS_V2 = [
    "Code",
    "ProdCat",
    "Date",
    "O",
    "H",
    "L",
    "C",
    "MO",
    "MH",
    "ML",
    "MC",
    "EO",
    "EH",
    "EL",
    "EC",
    "AO",
    "AH",
    "AL",
    "AC",
    "Vo",
    "OI",
    "Va",
    "CM",
    "VoOA",
    "EmMrgnTrgDiv",
    "LTD",
    "SQD",
    "Settle",
    "CCMFlag",
]

# ref. ja https://jpx-jquants.com/ja/spec/drv-bars-daily-opt
# ref. en https://jpx-jquants.com/en/spec/drv-bars-daily-opt
DERIVATIVES_OPTIONS_COLUMNS_V2 = [
    "Code",
    "ProdCat",
    "UndSSO",
    "Date",
    "O",
    "H",
    "L",
    "C",
    "MO",
    "MH",
    "ML",
    "MC",
    "EO",
    "EH",
    "EL",
    "EC",
    "AO",
    "AH",
    "AL",
    "AC",
    "Vo",
    "OI",
    "Va",
    "CM",
    "Strike",
    "VoOA",
    "EmMrgnTrgDiv",
    "PCDiv",
    "LTD",
    "SQD",
    "Settle",
    "Theo",
    "BaseVol",
    "UnderPx",
    "IV",
    "IR",
    "CCMFlag",
]

# ref. ja https://jpx-jquants.com/ja/spec/mkt-short-sale
# ref. en https://jpx-jquants.com/en/spec/mkt-short-sale
SHORT_SELLING_POSITIONS_COLUMNS_V2 = [
    "DiscDate",
    "CalcDate",
    "Code",
    "SSName",
    "SSAddr",
    "DICName",
    "DICAddr",
    "FundName",
    "ShrtPosToSO",
    "ShrtPosShares",
    "ShrtPosUnits",
    "PrevRptDate",
    "PrevRptRatio",
    "Notes",
]

# ref. ja https://jpx-jquants.com/ja/spec/mkt-margin-alert
# ref. en https://jpx-jquants.com/en/spec/mkt-margin-alert
DAILY_MARGIN_INTEREST_COLUMNS_V2 = [
    "PubDate",
    "Code",
    "AppDate",
    "PubReason",
    "ShrtOut",
    "ShrtOutChg",
    "ShrtOutRatio",
    "LongOut",
    "LongOutChg",
    "LongOutRatio",
    "SLRatio",
    "ShrtNegOut",
    "ShrtNegOutChg",
    "ShrtStdOut",
    "ShrtStdOutChg",
    "LongNegOut",
    "LongNegOutChg",
    "LongStdOut",
    "LongStdOutChg",
    "TSEMrgnRegCls",
]

# ref. ja https://jpx-jquants.com/ja/spec/bulk-list
# ref. en https://jpx-jquants.com/en/spec/bulk-list
BULK_LIST_COLUMNS_V2 = [
    "Key",
    "Size",
    "LastModified",
]

TD_LIST_COLUMNS_V2 = [
    "DiscNo",
    "Code",
    "Name",
    "DiscDate",
    "DiscTime",
    "Title",
    "DiscStatus",
    "RevNo",
    "DiscItems",
    "Docs",
]
