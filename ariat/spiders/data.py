import datetime
import os.path
from typing import Iterable

import scrapy
from scrapy import Request
from scrapy.cmdline import execute as ex
from ariat.db_config import DbConfig
from fake_useragent import UserAgent
import json
from ariat.items import dataItem
from ariat.common_func import create_md5_hash, page_write


ua = UserAgent()

obj = DbConfig()
today_date = datetime.datetime.today().strftime("%d_%m_%Y")
def headers():
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'referer': 'https://www.ariat.com/',
        'user-agent': ua.random,
    }
    return headers


class DataSpider(scrapy.Spider):
    name = "data"
    # allowed_domains = ["."]
    # start_urls = ["https://."]
    def start_requests(self):
        obj.cur.execute(f"select * from {obj.store_links_table} where status=0")
        rows = obj.cur.fetchall()
        for row in rows:
            link = row['link']
            tag = row['tag']
            hashid = create_md5_hash(link)
            pagesave_dir = rf"C:/Users/Actowiz/Desktop/pagesave/ariat/{today_date}"
            file_name = fr"{pagesave_dir}/{hashid}.html"
            row['hashid'] = hashid
            row['pagesave_dir'] = pagesave_dir
            row['file_name'] = file_name

            if os.path.exists(file_name):
                yield scrapy.Request(
                    url= 'file:///' + file_name,
                    cb_kwargs= row,
                    callback= self.parse
                )
            else:
                yield scrapy.Request(url= link,
                                     headers= headers(),
                                     callback= self.parse,
                                     cb_kwargs= row
                                 )

    def parse(self, response, **kwargs):
        file_name = kwargs['file_name']
        pagesave_dir = kwargs['pagesave_dir']
        hashid = kwargs['hashid']
        if not os.path.exists(file_name):
            page_write(pagesave_dir, file_name, response.text)
        store_name = response.xpath("//h1[contains(@class, 'brand-retail-title')]/text()").get()
        address = response.xpath("//div[contains(@class, 'landing-header-address')]/span[@class='landing-header-detail-item']/span/text()").getall()
        try:
            street = address[0]
        except: street = ''
        full_address = ', '.join(address)
        script_text = response.xpath("//script[@type='application/ld+json']/text()").get()
        script_text = script_text.strip()
        jsn = json.loads(script_text)
        store_name = jsn['name']
        phone = jsn['telephone']
        city = jsn['address']['addressLocality']
        region = jsn['address']['addressRegion']
        postalcode = jsn['address']['postalCode']
        lat = jsn['geo']['latitude']
        lng = jsn['geo']['longitude']
        try:
            list_of_schedule = list()

            openingHoursSpecification = jsn['openingHoursSpecification']
            for hour_specification in openingHoursSpecification:
                for daysofweek in hour_specification['dayOfWeek']:
                    day = daysofweek.capitalize()
                    schedule = f"{day}: {hour_specification['opens']}-{hour_specification['closes']}"
                    list_of_schedule.append(schedule)
            schedule_final = ' | '.join(list_of_schedule)
        except:
            schedule_final = ''

        direction_url = response.xpath('//div[@class="outbound-directions"]/a/@href').get()

        item = dataItem()
        item['store_no'] = ''
        item['name'] = store_name
        item['latitude'] = lat
        item['longitude'] = lng
        item['street'] = street
        item['city'] = city
        item['state'] = region
        item['zip_code'] = postalcode
        item['county'] = city
        item['phone'] = phone
        item['open_hours'] = schedule_final
        item['url'] = kwargs['link']
        item['provider'] = "Ariat"
        item['category'] = "Apparel And Accessory Stores"
        item['updated_date'] = datetime.datetime.today().strftime("%d-%m-%Y")
        item['country'] = "US"
        item['status'] = "Open"
        item['direction_url'] = direction_url
        item['pagesave_path'] = file_name

        yield item



if __name__ == '__main__':
    ex("scrapy crawl data".split())