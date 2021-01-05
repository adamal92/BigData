import re

import bs4
import scrapy
import logging

from bs4 import BeautifulSoup


class DirtyMotoSpider(scrapy.Spider):
    name = 'motors'
    start_urls = [
        'https://centro.co.il/en/bike/yamaha/'
        ,
        'https://centro.co.il/en/auto/'
        ,
        'https://www.kawasaki.co.il/motorbikes/supernaked/z900_35kw/?_'
        'ga=2.193938925.1786289935.1609773572-1769228414.1609773572'
    ]

    def parse(self, response, **kwargs):
        print(response)  # <200 https://centro.co.il/en/bike/yamaha/>
        if not response.url in DirtyMotoSpider.start_urls:
            logging.error("failed to access website")

        from bs4 import BeautifulSoup
        soup: BeautifulSoup = BeautifulSoup(str(response.css('body').getall().pop()), 'html.parser')
        # print(str(response.css('body').getall()))
        # print(soup.prettify())
        # for child in soup.body.children:
        #     # print(child.prettify())
        #     # # print(type(child))
        #     # print("\n---------child-----------------------------------------\n")
        #     DirtyMotoSpider.recurse_over_html_tree(soup_Tag=child)
        # DirtyMotoSpider.recurse_over_html_tree(soup_Tag=soup.body)

        # for tag in soup.find_all(lambda tag: tag.stripped_strings):
        for tag in soup.find_all(DirtyMotoSpider.filter_unstripped_strings):
            print("\n---------child-----------------------------------------\n")
            for string in tag.stripped_strings:
                print(string)

        next_page = response.css('div.next a::attr("href")').get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)

        next_page = response.css('li.next a::attr("href")').get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)

    @staticmethod
    def recurse_over_html_tree(soup_Tag: bs4.element.Tag):
        # if soup_Tag.children is None:
        #     print("\n---------child-----------------------------------------\n")
        #     print(soup_Tag.prettify())
        #     return
        try:
            # print(type(soup_Tag), type(soup_Tag.children))
            for child in soup_Tag.children:
                DirtyMotoSpider.recurse_over_html_tree(child)
        except:  # if child is a leaf
            # print(soup_Tag.prettify())
            # print(soup_Tag.string)
            # if soup_Tag.string.isalnum() and soup_Tag.string.strip():
            #     print("\n---------child-----------------------------------------\n")
            #     print(soup_Tag.string)  # .strip("\n\t\r\b\"@:?{}[]<>;.")
            # if text.strip(): print(text)  # .strip("\n\t\r\b\"@:?{}[]<>;.")
            # if re.search('[^A-Za-z0-9]', string=soup_Tag.string) and soup_Tag.string.strip():
            #     print("\n---------child-----------------------------------------\n")
            #     print(soup_Tag.string)  # .strip("\n\t\r\b\"@:?{}[]<>;.")
            for string in soup_Tag.stripped_strings:
                print("\n---------child-----------------------------------------\n")
                print(repr(string))
            print(soup_Tag.parent)

    @staticmethod
    def filter_unstripped_strings(tag: bs4.element.Tag) -> bool:
        has_special_chars: bool = False

        for string in tag.stripped_strings:
            if string is None or not string.strip():
                has_special_chars = True

        return not has_special_chars
