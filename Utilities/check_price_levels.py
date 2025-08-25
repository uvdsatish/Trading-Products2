from Utilities.price_levels_scanner import get_price_levels

# For long positions (shows highs)
get_price_levels('SBS', 'long')
#get_price_levels(['AAPL', 'MSFT'], 'long')

# For short positions (shows lows)
#get_price_levels('TSLA', 'short')
