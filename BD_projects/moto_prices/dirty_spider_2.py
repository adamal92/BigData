import re

import scrapy
import logging


class DirtyMotoSpider(scrapy.Spider):
    name = 'motors'
    start_urls = [
        'https://centro.co.il/en/bike/yamaha/'
    ]

    def parse(self, response, **kwargs):
        print(response)  # <200 https://centro.co.il/en/bike/yamaha/>
        if not response.url in DirtyMotoSpider.start_urls:
            logging.error("failed to access website")

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(str(response.css('body').getall().pop()), 'html.parser')
        # print(str(response.css('body').getall()))
        # print(soup.prettify())
        # for child in soup.body.children:
        #     # print(child.prettify())
        #     # # print(type(child))
        #     # print("\n---------child-----------------------------------------\n")
        #     DirtyMotoSpider.recurse_over_html_tree(soup_Tag=child)
        DirtyMotoSpider.recurse_over_html_tree(soup_Tag=soup.body)

        next_page = response.css('div.next a::attr("href")').get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)

        next_page = response.css('li.next a::attr("href")').get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)

    @staticmethod
    def recurse_over_html_tree(soup_Tag):
        # if soup_Tag.children is None:
        #     print("\n---------child-----------------------------------------\n")
        #     print(soup_Tag.prettify())
        #     return
        try:
            for child in soup_Tag.children:
                DirtyMotoSpider.recurse_over_html_tree(child)
        except:
            print("\n---------child-----------------------------------------\n")
            print(soup_Tag.prettify())
