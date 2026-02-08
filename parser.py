from lxml import html
import requests
from pprint import pprint
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3

class Parser:
    def __init__(self, base_url):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def get_page(self, url):
        response = self.session.get(url)
        return html.fromstring(response.text)
    
    def get_page_count(self, url):
        tree = self.get_page(self.base_url + url)
        page_count = int(tree.xpath('//span[@class="rt-Text rt-r-size-2"]/text()')[-1])
        return page_count

    def parse_categories_and_subcategories(self):
        url = "categories"
        tree = self.get_page(self.base_url + url)
        categories = {}
        items = tree.xpath('//div[@class="rt-reset rt-BaseCard rt-Card rt-r-size-2 sm:rt-r-size-3 rt-variant-surface"]')
        for item in items:
            category_name = item.xpath('.//h2[@class="rt-Heading rt-r-size-3 _cardTitle_uuwu4_11"]/text()')[0]
            subcategory_items = item.xpath('.//div[@class="rt-Box rt-r-pb-2"]')
            subcategories = []
            for subcategory_item in subcategory_items:
                subcategory_name = subcategory_item.xpath('.//a/text()')[0]
                subcategory_url = subcategory_item.xpath('.//a/@href')[0]
                subcategories.append([subcategory_name, subcategory_url])

            categories[category_name] = {"category_name": category_name, "subcategories": subcategories}
        return categories

    def parse_product(self, url):
        print(url)
        product_page = self.get_page(url)
        name = product_page.xpath('//h1[@class="rt-Heading rt-r-size-6 xs:rt-r-size-8"]/text()')
        low = product_page.xpath('//span[@class="v-fw-600 v-fs-12"]/text()')
        median = product_page.xpath('//div[@class="rt-Flex _rangeAverage_118fo_42"]/text()')
        high = product_page.xpath('//span[@class="_rangeSliderLastNumber_118fo_38 v-fw-600 v-fs-12"]/text()')
        description = product_page.xpath('//p[@class="rt-Text"]/text()')
        if name == [] or low == [] or median == [] or high == [] or description == []:
            if name == []:
                name = product_page.xpath('//h1[@class="rt-Heading rt-r-size-6 rt-r-weight-medium"]/text()')
                if name == []:
                    pass
                else:
                    product = {"name": name[0], "all_info": False}
            else:
                    product = {"name": name[0], "all_info": False}
        else:
            product = {"name": name[0], "low": low[0], "median": median[0], "high": high[0], "description": description[0], "all_info": True}
        try:
            return product
        except Exception as ex:
            print(ex)
    
    def parse_subcategory_products(self, subcategory, category_name):
        subcategory_products = []
        page_count = self.get_page_count(subcategory[1])
        for page_num in range(1, page_count + 1):
            subcategory_page = self.get_page(f"{self.base_url}{subcategory[1][:-1]}{page_num}")
            products_urls = subcategory_page.xpath('//a[@class="_card_gl3kq_9 _card_1u7u9_1 _cardLink_1q928_1"]/@href')
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(self.parse_product, self.base_url + url)
                    for url in products_urls
                ]
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result['all_info'] == True:
                            cursor.execute(f'INSERT INTO {category_name} (name, all_info, lowest_price, median_price, highest_price, description) VALUES (?, ?, ?, ?, ?, ?)', (result['name'], result['all_info'], result['low'], result['median'], result['high'], result['description']))
                        elif result['all_info'] == False:
                            cursor.execute(f'INSERT INTO {category_name} (name, all_info) VALUES (?, ?)', (result['name'], result['all_info']))
                        connection.commit()
                    except Exception as ex:
                        print(ex)

    def parse_products_from_subcategories(self, category, category_name):
        cursor.execute(f'''
CREATE TABLE IF NOT EXISTS {category_name} (
id INTEGER PRIMARY KEY,
name TEXT NOT NULL,
all_info BOOL NOT NULL,
lowest_price TEXT,
median_price TEXT,
highest_price TEXT,
description TEXT
)
''')   
        for subcategory in category['subcategories']:
            print(subcategory)
            self.parse_subcategory_products(subcategory=subcategory, category_name=category_name)


if __name__ == "__main__":
    connection = sqlite3.connect("database.db")
    cursor = connection.cursor()
    parser = Parser("https://www.vendr.com/")
    categories = parser.parse_categories_and_subcategories()
    parser.parse_products_from_subcategories(categories["DevOps"], "DevOps")
    parser.parse_products_from_subcategories(categories["IT Infrastructure"], "ITInfrastructure")
    parser.parse_products_from_subcategories(categories["Data Analytics and Management"], "DataAnalyticsandManagement")
    connection.close()