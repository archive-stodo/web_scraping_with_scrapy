import scrapy
import pandas as pd

# to run, just run
# PycharmProjects/finviz/finviz$ sscrapy crawl spider-name -o output.csv


class FinvizSpider(scrapy.Spider):
    name = 'spider-name'

    def start_requests(self):
        urls = ['https://finviz.com/screener.ashx?v=121&f=cap_smallover,fa_div_o3&o=industry']
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        content = response.xpath('//div[@id="screener-content"]')

        dark_rows = content.xpath('.//tr[@class="table-dark-row-cp"]')
        light_rows = content.xpath('.//tr[@class="table-light-row-cp"]')

        parsed_dark_rows = self.parseRows(dark_rows)
        parsed_light_rows = self.parseRows(light_rows)

        parsed_rows = parsed_light_rows + parsed_dark_rows

        df = pd.DataFrame(parsed_rows)
        print('hrhrhrhr')
        print(df)

        yield {'df': df}

    def parseRows(self, rows):
        rows_text = rows.css('td.screener-body-table-nw ::text').extract()
        rows_text = self.divide_chunks(rows_text, 18)

        parsed_rows = []
        for index, row in enumerate(rows_text):
            row_dict = dict()
            row_dict['id'] = row[0]
            row_dict['ticker'] = row[1]
            row_dict['market_cap'] = row[2]
            row_dict['p/s'] = row[6]
            row_dict['p/b'] = row[7]
            row_dict['p/c'] = row[8]
            row_dict['price'] = row[15]
            parsed_rows.append(row_dict)

        return parsed_rows

    def divide_chunks(self, l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]
