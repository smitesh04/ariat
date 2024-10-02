import scrapy
from scrapy.cmdline import execute as ex
from ariat.items import storeLinksItem


class StoreLinksSpider(scrapy.Spider):
    name = "store_links"
    # allowed_domains = ["."]
    start_urls = ["https://www.ariat.com/brandshops?srsltid=AfmBOoq7nIxa3J_XxbDXKiXEFqQP7HUl-8hnudCcNRNqti6WOTPQSyY8"]

    def parse(self, response):
        store_card_div = response.xpath("//div[contains(@class,'holds-1-link')]")
        for store_card in store_card_div:
            store_tag = store_card.xpath(".//div[contains(@class,'pd-header-inner')]/text()").get()
            try:store_description = store_card.xpath(".//div[@class='pd-description']/p/span/text()").get()
            except:store_description = ''
            store_url = store_card.xpath(".//a/@href").get()

            item = storeLinksItem()
            item['tag'] = store_tag.strip()
            item['description'] = store_description
            item['link'] = store_url

            yield item


        
if __name__ == '__main__':
    ex("scrapy crawl store_links".split())