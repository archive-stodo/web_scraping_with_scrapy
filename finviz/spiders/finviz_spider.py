import scrapy
import pandas as pd

# to run, just run
# PycharmProjects/finviz/finviz$ scrapy crawl spider-name
from scrapy import Selector


class FinvizSpider(scrapy.Spider):
    name = 'spider-name'

    screener_url = 'https://finviz.com/screener.ashx'
    overview_screen = '?v=111'
    valuation_screen = '?v=121'
    not_small_with_good_dividend = '&f=cap_smallover,fa_div_o3&o=industry'

    valuation_url = screener_url + valuation_screen + not_small_with_good_dividend
    overview_url = screener_url + overview_screen + not_small_with_good_dividend

    def start_requests(self):
        urls = ['https://finviz.com/screener.ashx?v=121&f=cap_smallover,fa_div_o3&o=industry']
        for url in urls:
            yield scrapy.Request(url=url, callback=self.scrap_valuation)

        yield scrapy.Request(url=FinvizSpider.overview_url, callback=self.scrap_overview)

    def scrap_overview(self, response):
        content, parsed_rows = self.parse_data_rows_on_website(response, self.parse_overview_columns)
        self.save_rows_array_to_file(parsed_rows, 'overview.txt')

        next_page_a_href = content.css('a[href].tab-link').extract()[-1]
        sel = Selector(text=next_page_a_href)
        link = 'https://finviz.com/' + sel.css('a::attr(href)').extract()[0]
        print('next_page_link ', link)

        yield scrapy.Request(url=link, callback=self.scrap_overview)

    def parse_overview_columns(self, response):
        print('We are actually here!')
        rows_text = response.css('td.screener-body-table-nw ::text').extract()
        rows_text = self.divide_chunks(rows_text, 11)

        parsed_rows = []
        for index, row in enumerate(rows_text):
            row_dict = dict()
            row_dict['id'] = row[0]
            row_dict['sector'] = row[3]
            row_dict['p/e'] = row[7]

            parsed_rows.append(row_dict)

        return parsed_rows

    def scrap_valuation(self, response):
        content, parsed_rows = self.parse_data_rows_on_website(response, self.parse_valuation_columns)
        self.save_rows_array_to_file(parsed_rows, 'output.txt')

        next_page_a_href = content.css('a[href].tab-link').extract()[-1]
        sel = Selector(text=next_page_a_href)
        link = 'https://finviz.com/' + sel.css('a::attr(href)').extract()[0]
        print('next_page_link ', link)

        yield scrapy.Request(url=link, callback=self.scrap_valuation)

    def parse_valuation_columns(self, response):
        rows_text = response.css('td.screener-body-table-nw ::text').extract()
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

 #  -----------------  Common Functions  ----------------------------------------

    def parse_data_rows_on_website(self, response, column_parsing_function):
        content = response.xpath('//div[@id="screener-content"]')
        dark_rows = content.xpath('.//tr[@class="table-dark-row-cp"]')
        light_rows = content.xpath('.//tr[@class="table-light-row-cp"]')
        parsed_dark_rows = column_parsing_function(dark_rows)
        parsed_light_rows = column_parsing_function(light_rows)
        parsed_rows = parsed_light_rows + parsed_dark_rows
        return content, parsed_rows

    def save_rows_array_to_file(self, parsed_rows, filename):
        df = pd.DataFrame(parsed_rows)
        df.set_index('id', inplace=True)
        df.to_csv(filename, mode='a', header=False, index=True)

    def divide_chunks(self, l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]
