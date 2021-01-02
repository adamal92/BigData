import scrapy
import logging


class MotorSpider(scrapy.Spider):
    name = 'motors'
    start_urls = [
        'https://centro.co.il/en/bike/yamaha/',
    ]

    def parse(self, response, **kwargs):
        # print(response.css('li.mdl-list__item'))
        # logging.getLogger('my_log').error("ok")

        for motorcycle in response.css('li.mdl-list__item'):
            # print(motorcycle.xpath('div/a/strong/text()').get())
            # print(motorcycle.xpath('a/p/text()').get())
            # print(motorcycle.xpath('div/p/span').extract())

            motorcycle_obj = {}

            # for fact in motorcycle.xpath('div/p/span').extract():
            #     print(fact)
            #     print(type(motorcycle.xpath('div/p/span')))
            #     print(motorcycle.xpath('div/p[@class="opts"]/span').css('span::text').extract())

            # print(motorcycle.xpath('div/p[@class="opts"]/span').css('span::text').extract())
            # print(motorcycle.xpath('div/p[@class="opts"]/span[@class="engine"]').css('span::text').get())

            motorcycle_obj["model"] = motorcycle.xpath('div/a/strong/text()').get()
            motorcycle_obj["price"] = motorcycle.xpath('a/p/text()').get()
            motorcycle_obj["all info"] = motorcycle.xpath('div/p[@class="opts"]/span').css('span::text').extract()
            motorcycle_obj["engine"] = motorcycle.xpath('div/p[@class="opts"]/span[@class="engine"]') \
                .css('span::text').get()
            motorcycle_obj["mileage"] = motorcycle.xpath('div/p[@class="opts"]/span[@class="mileage"]') \
                .css('span::text').get()
            motorcycle_obj["gears"] = motorcycle.xpath('div/p[@class="opts"]/span[@class="gears"]') \
                .css('span::text').get()
            motorcycle_obj["color"] = motorcycle.xpath('div/p[@class="opts"]/span[@class="color"]') \
                .css('span::text').get()

            yield motorcycle_obj

            # yield {
            #     "model": motorcycle.xpath('div/a/strong/text()').get(),
            #     "price": motorcycle.xpath('a/p/text()').get(),
            #     "engine": motorcycle.xpath('div/p/span.engine/text()').get(),
            #     "mileage": motorcycle.xpath('div/p/span.mileage/text()').get(),
            #     "gears": motorcycle.xpath('div/p/span.gears/text()').get()
            # }

        next_page = response.css('div.next a::attr("href")').get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)



    # def parse(self, response, **kwargs):
    #     # yield {'price': response.css('p.price::text').extract()}  # ::text
    #     for price in response.css('p.price::text').extract():
    #         print(price)
    #         yield {
    #             "price": price
    #         }
    #
    #     print(response.css('a.name').extract())
    #     print(response.css('a.name::text'))
    #     # print(response.css('a.name').get().xpath('strong/text()'))
    #
    #     for name in response.css('a.name'):
    #         print(name.xpath('strong/text()').get())
    #
    #     #
    #     # for price in response.css('p.price'):
    #     #     yield {
    #     #         # 'price2': price.xpath('span/small/text()').get(),
    #     #         # 'price': price.css('p.price > span::text').extract(),
    #     #         'price3': price.css('span.text::text').get(),
    #     #     }
    #     #
    #     # yield {'price': response.css('p.price')}
    #
    #     # # get next page
    #     # next_page = response.css('li.next a::attr("href")').get()
    #     # if next_page is not None:
    #     #     yield response.follow(next_page, self.parse)

    # def parse(self, response, **kwargs):
    #     motorcycles = {}
    #
    #     for name in response.css('a.name'):
    #         print(name.xpath('strong/text()').get())
    #         moto_name: str = name.xpath('strong/text()').get()
    #         motorcycles[moto_name] = {}
    #
    #     for price in response.css('p.price::text').extract():
    #          print(price)
    #          motorcycles[]
    #
    #      print(response.css('a.name').extract())
    #      print(response.css('a.name::text'))
    #
    #
    #      yield motorcycles


        # for name in response.css('a.name'):
        #     print(name.xpath('strong/text()').get())
        #     moto_name: str = name.xpath('strong/text()').get()
        #     motorcycles[moto_name] = {}
        #
        # for price in response.css('p.price::text').extract():
        #     print(price)
        #     # motorcycles[]
        #
        # print(response.css('a.name').extract())
        # print(response.css('a.name::text'))

