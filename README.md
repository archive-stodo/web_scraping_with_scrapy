# finviz_web_scraping_with_scrapy
Scraping multiple pages of finviz stocks fundamental data with Scrapy

initial url to start with:
'https://finviz.com/screener.ashx?v=121&f=cap_smallover,fa_div_o3&o=industry'

This url already applies 2 filters:
 - stocks with $300m market capitalization and bigger
 - dividend yield at least 3%
 
Scraping is saved in output.csv file inside the project:
      "   ticker market_cap   p/s    p/b    p/c   price
      id                                              
      2     UNM      6.05B  0.51   0.64  78.62   29.42
      4     WPP     17.34B  0.85   1.37      -   69.14
      6     NTR     27.84B  1.43   1.21      -   48.60
      ...
