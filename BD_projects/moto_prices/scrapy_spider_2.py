import scrapy
import logging


class MotorSpider(scrapy.Spider):
    name = 'motors'
    start_urls = [
        # bikes
        'https://centro.co.il/en/bike/yamaha/'
        ,
        "https://centro.co.il/en/bike/ducati/"
        ,
        "https://centro.co.il/en/bike/bmw/"
        ,
        "https://centro.co.il/en/bike/sanyang/"
        ,
        "https://centro.co.il/en/bike/peugeot/"
        
        # cars
        'https://centro.co.il/en/auto/'
        ,
        "https://centro.co.il/en/auto/mazda/"
        ,
        "https://centro.co.il/en/auto/ford/"
        ,
        "https://centro.co.il/en/auto/fiat/"

        # commercial
        "https://centro.co.il/en/util/"
        ,
        "https://centro.co.il/en/util/volkswagen/"
    ]

    def parse(self, response, **kwargs):
        print(response)

        moto_obj = {}

        for motorcycle in response.css('li.mdl-list__item'):
            # moto_obj["type"] = "motorcycle"

            moto_obj["model"] = motorcycle.xpath('div/a/strong/text()').get()
            moto_obj["price"] = motorcycle.xpath('a/p/text()').get()
            moto_obj["all info"] = motorcycle.xpath('div/p[@class="opts"]/span').css('span::text').extract()
            moto_obj["engine"] = motorcycle.xpath('div/p[@class="opts"]/span[@class="engine"]') \
                .css('span::text').get()
            moto_obj["mileage"] = motorcycle.xpath('div/p[@class="opts"]/span[@class="mileage"]') \
                .css('span::text').get()
            moto_obj["gears"] = motorcycle.xpath('div/p[@class="opts"]/span[@class="gears"]') \
                .css('span::text').get()
            moto_obj["color"] = motorcycle.xpath('div/p[@class="opts"]/span[@class="color"]') \
                .css('span::text').get()

            yield moto_obj

        for car in response.css('div.mdl-cell--2-col'):
            # moto_obj["type"] = "car"

            moto_obj["price"] = car.css('span.price::text').get()
            moto_obj["model"] = car.css('span.name::text').get()

            yield moto_obj

        # from bs4 import BeautifulSoup
        # soup: BeautifulSoup = BeautifulSoup(str(response.css('body').getall().pop()), 'html.parser')
        # for span_tag in soup.find_all("span"):
        #     if span_tag.text:
        #         # print(type(span_tag.parent))
        #         try:
        #             for class_name in ["opts", "color"]:
        #                 if class_name in span_tag.parent['class']:
        #                     print(span_tag.parent['class'])
        #                     print(span_tag.text)
        #         except:
        #             pass
        #             # print(span_tag.parent)

        # yield moto_obj

        next_page = response.css('div.next a::attr("href")').get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)

        next_page = response.css('li.next a::attr("href")').get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)
