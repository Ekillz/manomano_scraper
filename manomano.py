#!/usr/bin/env python2.7
# -*- coding: utf_8 -*-

""" ManoMano Category Scraper: Scrapy spider for website manomano.fr
This script crawls the target website based on a list of category Url's.
Scraped data includes ean, title, hierarchy, brand and description.
"""

from alba import items
import scrapy.http.request
from alba.spiders.core import BaseSpider
import urlparse
import re

class ManomanoCategory(scrapy.Spider):
    allowed_domains = ["manomano.fr"]
    name = "manomano_category"
    handle_httpstatus_list = [403, 503, 429, 504, 502, 500]
    crawlera_enabled = True
    crawlera_apikey = ''
    custom_settings = {
        "RETRY_HTTP_CODES": [403, 503, 429, 504, 502, 500],
        "CONCURRENT_REQUESTS": 12,
        "DOWNLOADER_MIDDLEWARES": {
            'scrapy_crawlera.CrawleraMiddleware': 610
        }
    }

    def __init__(self, start, end):
        """
        Gets a list of urls from a mysql table and transforms them into a dict.

        :param start: starting index of the list
        :param end: ending index of the list
        """
        df = BaseSpider.select_table("filtered.manomano_category", ["url"], as_df=True)
        self.base_url = 'https://www.manomano.fr'
        self.remote_data = df.to_dict(orient='records')
        self.remote_data = self.remote_data[int(start) : int(end)]
        pass

    def get_absolute_url(self, url):
        return urlparse.urljoin(self.root_url, url)

    def start_requests(self):
        """
        Request for the main page based on category urls in remote_data
        :return:
        """

        for row in self.remote_data:
            url = row['url']
            request = scrapy.http.request.Request(url, callback=self.parse_category)
            request.meta['category'] = url
            yield request

    def parse_category(self, response):
        """
        Gets the number of product pages, and loops one by one
        :param response:
        :return:
        """
        url = response.meta['category']

        try:
            max_page = response.xpath('(//a[@class="pagination__link js-listing__trigger"])[last() - 1]/text()').extract_first()
            max_page = int(max_page)
        except:
            self.logger.info("SINGLE PAGE, URL: {}".format(response.url))
            max_page = 2

        for i in range(1, max_page + 1):
            request = scrapy.http.request.Request(url + "?page=" + str(i), callback=self.parse_page, dont_filter=True)
            request.meta['category'] = url
            yield request

    def parse_page(self, response):
        """
        Gets all the product page urls.

        :param response:
        :return:
        """
        urls = response.xpath('//div[@class="product-list__product product-card js-product-card"]//@href').extract()
        for url in urls:
            request = scrapy.http.request.Request(self.base_url + url, callback=self.parse_product)
            request.meta['category'] = response.meta['category']
            yield request


    def parse_product(self, response):
        """
        Scrapes all the information on single product page

        :param response:
        :return:
        """
        item = items.ManomanoCategoryItem()
        item['category'] = response.meta['category']

        item['ean'] = response.xpath("//@data-flix-ean").extract_first()

        item['title'] = response.xpath("//h1[@class='product-info__name']/text()").extract_first()

        # Getting product's category hierarchy
        ariane = response.xpath('//ul[@class="breadcrumbs product__breadcrumbs-top"]/li/a/span/text()').extract()
        ariane = [x.strip() for x in ariane if x.strip()]
        ariane = " > ".join(ariane)
        item['ariane'] = ariane

        item['description'] = response.xpath('normalize-space(//div[@class="product-section__content product-section__content--padding"])').extract_first()

        # getting images urls and numbering them
        for idx, image in enumerate(response.xpath('//div[@class="product__images"]//@data-image').extract()):
            item['image_url_' + str(idx)] = image

        item['brand'] = response.xpath('//span[@itemprop="brand"]/text()').extract_first()

        # Getting a cleaner category, sometimes it ends with a '-' which needs to be removed
        category = item['category'].split('/')[-1]
        category = re.sub('[^aA-zZ-]', "", category)
        if category[-1] == '-':
            category = category[:-1]
        item['category'] = category

        yield item
