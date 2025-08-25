import sys
from breakout_scanner import check_52week_breakouts
from breakout_scanner_25day import check_25day_breakouts
from breakout_scanner_100day import check_100day_breakouts

# Your ticker list

my_tickers = [
    # Longs delta
    "AFRM", "AG", "AGX", "ARKF", "ARKK", "ARKQ", "ARKW", "ARKX", "ARMN",
    "BUZZ", "CALM", "CCEC", "CCJ", "COHR", "COPJ", "CRDO", "CRPT", "CVNA",
    "DASH", "DAVE", "EPOL", "FIX", "FPX", "FTI", "HOOD", "HSAI", "IBIT",
    "IZRL", "JOYY", "KB", "PBI", "PIZ", "PRA", "PSIL", "PSIX", "PWRD",
    "QTUM", "SAMT", "SBSW", "SEZL", "SHOC", "SITM", "TOST", "TSSI", "TTMI",
    "URBN", "UTES", "VIRT", "YSG",

    # Shorts delta
    "CABO", "CAR", "CGON", "CRNX", "GPCR", "HELE", "JANX", "MAGN", "MLAB",
    "OLN", "RARE", "RVMD", "SNDX", "USPH", "VERA", "WGO", "ZIP"
]
results_yearly = check_52week_breakouts(my_tickers)

results_25 = check_25day_breakouts(my_tickers)

results_100 = check_100day_breakouts(my_tickers)

sys.exit(0)

