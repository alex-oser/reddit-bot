#!/bin/bash

function refresh_stock_data() {
	curl 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download' > $amex
	curl 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download' > $nyse
	curl 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download' > $nasdaq
	sed -i'.bak' 's/Summary Quote/summary_quote/g' $amex
	sed -i'.bak' 's/Summary Quote/summary_quote/g' $nyse
	sed -i'.bak' 's/Summary Quote/summary_quote/g' $nasdaq
	sqlite3 $db_name <<COMMANDS
.mode csv
.import $amex temp_info
.import $nyse temp_info
.import $nasdaq temp_info
INSERT OR IGNORE INTO stock_info
	SELECT Symbol,Name,LastSale,MarketCap,IPOyear,Sector,industry,summary_quote
	FROM temp_info;
DROP TABLE temp_info
COMMANDS
	rm ${amex}.bak ${nyse}.bak ${nasdaq}.bak
}

cwd=$(dirname ${BASH_SOURCE[0]})
db_name=reddit_parser.db
db=$cwd/$db_name
amex=$cwd/amex.csv
nyse=$cwd/nyse.csv
nasdaq=$cwd/nasdaq.csv
refresh_stock_data
