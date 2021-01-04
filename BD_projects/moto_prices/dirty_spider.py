import re

import scrapy
import logging


class DirtyMotoSpider(scrapy.Spider):
    name = 'motors'
    start_urls = [
        'https://centro.co.il/en/bike/yamaha/'
        # ,
        # 'https://centro.co.il/en/auto/'
        # ,
        # 'https://www.yad2.co.il/vehicles/motorcycles',  # protected by captcha
        # 'https://www.facebook.com/marketplace/category/vehicles',
        # 'https://fullgaz.co.il/category/%d7%9e%d7%9b%d7%95%d7%a0%d7%95%d7%aa/%d7%a8%d7%9b%d7%99%'
        # 'd7%91%d7%94-%d7%a8%d7%90%d7%a9%d7%95%d7%a0%d7%94/',
        # 'https://www.kawasaki.co.il/motorbikes/supernaked/z900_35kw/?_'
        # 'ga=2.193938925.1786289935.1609773572-1769228414.1609773572'
    ]

    def parse(self, response, **kwargs):
        # print(response.css('li.mdl-list__item'))
        # logging.getLogger('my_log').error("ok")
        print(response)  # <200 https://centro.co.il/en/bike/yamaha/>
        # print(response.url)
        if not response.url in DirtyMotoSpider.start_urls:
            # raise Exception("failed to access website")
            logging.error("failed to access website")
        # print(type(response))  # <class 'scrapy.http.response.html.HtmlResponse'>

#         # for motorcycle in response.css('li.mdl-list__item'):
#         #     # print(motorcycle.xpath('div/a/strong/text()').get())
#         #     # print(motorcycle.xpath('a/p/text()').get())
#         #     # print(motorcycle.xpath('div/p/span').extract())
#         #
#         #     motorcycle_obj = {}
#         #
#         #     # for fact in motorcycle.xpath('div/p/span').extract():
#         #     #     print(fact)
#         #     #     print(type(motorcycle.xpath('div/p/span')))
#         #     #     print(motorcycle.xpath('div/p[@class="opts"]/span').css('span::text').extract())
#         #
#         #     # print(motorcycle.xpath('div/p[@class="opts"]/span').css('span::text').extract())
#         #     # print(motorcycle.xpath('div/p[@class="opts"]/span[@class="engine"]').css('span::text').get())
#         #
#         #     motorcycle_obj["model"] = motorcycle.xpath('div/a/strong/text()').get()
#         #     motorcycle_obj["price"] = motorcycle.xpath('a/p/text()').get()
#         #     motorcycle_obj["all info"] = motorcycle.xpath('div/p[@class="opts"]/span').css('span::text').extract()
#         #     motorcycle_obj["engine"] = motorcycle.xpath('div/p[@class="opts"]/span[@class="engine"]') \
#         #         .css('span::text').get()
#         #     motorcycle_obj["mileage"] = motorcycle.xpath('div/p[@class="opts"]/span[@class="mileage"]') \
#         #         .css('span::text').get()
#         #     motorcycle_obj["gears"] = motorcycle.xpath('div/p[@class="opts"]/span[@class="gears"]') \
#         #         .css('span::text').get()
#         #     motorcycle_obj["color"] = motorcycle.xpath('div/p[@class="opts"]/span[@class="color"]') \
#         #         .css('span::text').get()

        # print(response.css('div').css('span::text'))  # SelectorList[Selector]
        # print(response.css('*'))  # get all html from page
        # for motorcycle in response.css('div'):
        #     print(motorcycle)
        #     motorcycle_obj = {motorcycle}
        #     yield motorcycle_obj

        # for selector in response.css('*').getall():
        #     pass
        #     # print(selector)

        # print(response.css('span:last-child::text').get())
        # print(response.css('*:empty').getall())

        # for text in response.css('::text').getall():
        # #     # re.sub('[^A-Za-z0-9]+', '', mystring)
        # #     # ''.join(e for e in string if e.isalnum())
        #     if text.isalnum() and text.strip(): print(text)  # .strip("\n\t\r\b\"@:?{}[]<>;.")
        #     # if text.strip(): print(text)  # .strip("\n\t\r\b\"@:?{}[]<>;.")
        #     # if re.search('[^A-Za-z0-9]', string=text): print(text)  # .strip("\n\t\r\b\"@:?{}[]<>;.")

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(str(response.css('*')), 'html.parser')


        next_page = response.css('div.next a::attr("href")').get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)

        next_page = response.css('li.next a::attr("href")').get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)

#     # yield {
#     #     "model": motorcycle.xpath('div/a/strong/text()').get(),
#     #     "price": motorcycle.xpath('a/p/text()').get(),
#     #     "engine": motorcycle.xpath('div/p/span.engine/text()').get(),
#     #     "mileage": motorcycle.xpath('div/p/span.mileage/text()').get(),
#     #     "gears": motorcycle.xpath('div/p/span.gears/text()').get()
#     # }
#     # def parse(self, response, **kwargs):
#     #     # yield {'price': response.css('p.price::text').extract()}  # ::text
#     #     for price in response.css('p.price::text').extract():
#     #         print(price)
#     #         yield {
#     #             "price": price
#     #         }
#     #
#     #     print(response.css('a.name').extract())
#     #     print(response.css('a.name::text'))
#     #     # print(response.css('a.name').get().xpath('strong/text()'))
#     #
#     #     for name in response.css('a.name'):
#     #         print(name.xpath('strong/text()').get())
#     #
#     #     #
#     #     # for price in response.css('p.price'):
#     #     #     yield {
#     #     #         # 'price2': price.xpath('span/small/text()').get(),
#     #     #         # 'price': price.css('p.price > span::text').extract(),
#     #     #         'price3': price.css('span.text::text').get(),
#     #     #     }
#     #     #
#     #     # yield {'price': response.css('p.price')}
#     #
#     #     # # get next page
#     #     # next_page = response.css('li.next a::attr("href")').get()
#     #     # if next_page is not None:
#     #     #     yield response.follow(next_page, self.parse)
#
#     # def parse(self, response, **kwargs):
#     #     motorcycles = {}
#     #
#     #     for name in response.css('a.name'):
#     #         print(name.xpath('strong/text()').get())
#     #         moto_name: str = name.xpath('strong/text()').get()
#     #         motorcycles[moto_name] = {}
#     #
#     #     for price in response.css('p.price::text').extract():
#     #          print(price)
#     #          motorcycles[]
#     #
#     #      print(response.css('a.name').extract())
#     #      print(response.css('a.name::text'))
#     #
#     #
#     #      yield motorcycles
#
#
#         # for name in response.css('a.name'):
#         #     print(name.xpath('strong/text()').get())
#         #     moto_name: str = name.xpath('strong/text()').get()
#         #     motorcycles[moto_name] = {}
#         #
#         # for price in response.css('p.price::text').extract():
#         #     print(price)
#         #     # motorcycles[]
#         #
#         # print(response.css('a.name').extract())
#         # print(response.css('a.name::text'))

