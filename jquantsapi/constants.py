# ref. ja https://jpx.gitbook.io/j-quants-api/api-reference/listed-api/17-sector
# ref. en https://jpx.gitbook.io/j-quants-api-en/api-reference/listed-api/17-sector
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

# ref. ja https://jpx.gitbook.io/j-quants-api/api-reference/listed-api/33-sector
# ref. en https://jpx.gitbook.io/j-quants-api-en/api-reference/listed-api/33-sector
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


# ref. ja https://jpx.gitbook.io/j-quants-api/api-reference/listed-api/segment
# ref. en https://jpx.gitbook.io/j-quants-api-en/api-reference/listed-api/segment
MARKET_SEGMENT_COLUMNS = [
    "MarketCode",
    "MarketCodeName",
    "MarketCodeNameEnglish",
]
MARKET_SEGMENT_DATA = [
    ("101", "東証一部", "1st Section"),
    ("102", "東証二部", "2nd Section"),
    ("104", "マザーズ", "Mothers"),
    ("105", "TOKYO PRO MARKET", "TOKYO PRO MARKET"),
    ("106", "JASDAQ スタンダード", "JASDAQ Standard"),
    ("107", "JASDAQ グロース", "JASDAQ Growth"),
    ("109", "その他", "Others"),
    ("111", "プライム", "Prime"),
    ("112", "スタンダード", "Standard"),
    ("113", "グロース", "Growth"),
]
