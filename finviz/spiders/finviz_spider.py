import scrapy
import pandas as pd
import logging
from scrapy import Selector


class FinvizSpider(scrapy.Spider):
    name = 'spider-name'

    # https://finviz.com/screener.ashx?v=171&f=cap_smallover,fa_div_o3&o=industry

    screener_url = 'https://finviz.com/screener.ashx'
    overview_screen = '?v=111'
    valuation_screen = '?v=121'
    financial_screen = '?v=161'
    ownership_screen = '?v=131'
    technical_screen = '?v=171'

    not_small_with_good_dividend = '&f=cap_smallover,fa_div_o3&o=industry'

    overview_url = screener_url + overview_screen
    valuation_url = screener_url + valuation_screen
    financial_url = screener_url + financial_screen
    ownership_url = screener_url + ownership_screen
    technical_url = screener_url + technical_screen

    def __init__(self):
        super().__init__()
        logging.basicConfig(level=logging.INFO)
        logging.info('This will get logged')

    def start_requests(self):
        url_filter = FinvizSpider.not_small_with_good_dividend

        yield scrapy.Request(url=FinvizSpider.technical_url, callback=self.scrap_technical)
        yield scrapy.Request(url=FinvizSpider.overview_url, callback=self.scrap_overview)
        yield scrapy.Request(url=FinvizSpider.valuation_url, callback=self.scrap_valuation)
        yield scrapy.Request(url=FinvizSpider.financial_url, callback=self.scrap_financial)
        yield scrapy.Request(url=FinvizSpider.ownership_url, callback=self.scrap_ownership)

    def scrap_technical(self, response):
        content, parsed_rows = self.parse_data_rows_on_website(response, self.parse_technical_columns)
        self.save_rows_array_to_file(parsed_rows, 'technical.txt')

        link = self.get_next_page_link(content)

        yield scrapy.Request(url=link, callback=self.scrap_technical)

    def parse_technical_columns(self, response):
        rows_text = response.css('td.screener-body-table-nw ::text').extract()
        rows_text = self.divide_chunks(rows_text, 15)

        parsed_rows = []
        for index, row in enumerate(rows_text):
            row_dict = dict()
            row_dict['id'] = row[0]
            row_dict['ticker'] = row[1]

            row_dict['beta'] = row[2]
            row_dict['sma20'] = row[4]
            row_dict['sma50'] = row[5]
            row_dict['sma200'] = row[6]
            row_dict['rsi'] = row[9]

            parsed_rows.append(row_dict)

        return parsed_rows

    def scrap_ownership(self, response):
        content, parsed_rows = self.parse_data_rows_on_website(response, self.parse_ownership_columns)
        self.save_rows_array_to_file(parsed_rows, 'ownership.txt')

        link = self.get_next_page_link(content)

        yield scrapy.Request(url=link, callback=self.scrap_ownership)

    def parse_ownership_columns(self, response):
        rows_text = response.css('td.screener-body-table-nw ::text').extract()
        rows_text = self.divide_chunks(rows_text, 15)

        parsed_rows = []
        for index, row in enumerate(rows_text):
            row_dict = dict()
            row_dict['id'] = row[0]
            row_dict['ticker'] = row[1]

            row_dict['outstanding'] = row[3]
            row_dict['float'] = row[4]
            row_dict['insiderOwn'] = row[5]
            row_dict['insiderTrans'] = row[6]
            row_dict['institutionOwn'] = row[7]
            row_dict['institutionTrans'] = row[8]

            row_dict['floatShort'] = row[9]
            row_dict['shortRatio'] = row[10]
            row_dict['avgVolume'] = row[11]

            parsed_rows.append(row_dict)

        return parsed_rows

    def scrap_financial(self, response):
        content, parsed_rows = self.parse_data_rows_on_website(response, self.parse_financial_columns)
        self.save_rows_array_to_file(parsed_rows, 'financial.txt')

        link = self.get_next_page_link(content)

        yield scrapy.Request(url=link, callback=self.scrap_financial)

    def parse_financial_columns(self, response):
        rows_text = response.css('td.screener-body-table-nw ::text').extract()
        rows_text = self.divide_chunks(rows_text, 18)

        parsed_rows = []
        for index, row in enumerate(rows_text):
            row_dict = dict()
            row_dict['id'] = row[0]
            row_dict['ticker'] = row[1]

            row_dict['dividend'] = row[3]
            row_dict['roa'] = row[4]
            row_dict['roe'] = row[5]
            row_dict['roi'] = row[6]

            row_dict['currR'] = row[7]
            row_dict['quickR'] = row[8]
            row_dict['ltDebt/Eq'] = row[9]
            row_dict['debt/eq'] = row[10]

            row_dict['grossM'] = row[11]
            row_dict['operM'] = row[12]
            row_dict['profitM'] = row[13]

            row_dict['volume'] = row[17]

            parsed_rows.append(row_dict)

        return parsed_rows

    def scrap_overview(self, response):
        content, parsed_rows = self.parse_data_rows_on_website(response, self.parse_overview_columns)
        self.save_rows_array_to_file(parsed_rows, 'overview.txt')

        link = self.get_next_page_link(content)

        yield scrapy.Request(url=link, callback=self.scrap_overview)

    def parse_overview_columns(self, response):
        rows_text = response.css('td.screener-body-table-nw ::text').extract()
        rows_text = self.divide_chunks(rows_text, 11)

        parsed_rows = []
        for index, row in enumerate(rows_text):
            row_dict = dict()
            row_dict['id'] = row[0]
            row_dict['ticker'] = row[1]

            row_dict['sector'] = row[3]
            row_dict['p/e'] = row[7]

            parsed_rows.append(row_dict)

        return parsed_rows

    def scrap_valuation(self, response):
        content, parsed_rows = self.parse_data_rows_on_website(response, self.parse_valuation_columns)
        self.save_rows_array_to_file(parsed_rows, 'valuation.txt')

        link = self.get_next_page_link(content)

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
            row_dict['p_to_s'] = row[6]
            row_dict['p_to_b'] = row[7]
            row_dict['p_to_c'] = row[8]
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

    def get_next_page_link(self, content):
        next_page_a_href = content.css('a[href].tab-link').extract()[-1]
        sel = Selector(text=next_page_a_href)
        link = 'https://finviz.com/' + sel.css('a::attr(href)').extract()[0]
        return link

    def save_rows_array_to_file(self, parsed_rows, filename):
        df = pd.DataFrame(parsed_rows)
        df.set_index('id', inplace=True)
        df.to_csv(filename, mode='a', header=False, index=True)
        logging.info(f'Zero or more rows added to file: {filename}')

    def divide_chunks(self, l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]
