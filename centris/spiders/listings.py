import re
from urllib.parse import urljoin
import scrapy
import json
from scrapy.selector import Selector

class ListingsSpider(scrapy.Spider):
    name = 'listings'
    allowed_domains = ['www.centris.ca']
    
    position = {
        'startPosition': 0
    }
    
    def start_requests(self):
        yield scrapy.Request(
            url = 'https://www.centris.ca/UserContext/Lock',
            method = 'POST',
            headers = {
                'Content-Type': 'application/json',
                'x-requested-with': 'XMLHttpRequest',
            },
            body = json.dumps({'uc':0}),
            callback = self.generate_uck
        )
    
    def generate_uck(self,response):
        uck = response.body
        query = {
            "query":{
                "UseGeographyShapes":0,
                "Filters":[
                    {
                        "MatchType":"CityDistrictAll",
                        "Text":"Montréal (All boroughs)",
                        "Id":5
                    }
                ],
                "FieldsValues":[
                    {
                        "fieldId":"CityDistrictAll",
                        "value":5,
                        "fieldConditionId":"",
                        "valueConditionId":""
                    },
                    {
                        "fieldId":"Category",
                        "value":"Residential",
                        "fieldConditionId":"",
                        "valueConditionId":""
                    },
                    {
                        "fieldId":"SellingType",
                        "value":"Rent",
                        "fieldConditionId":"",
                        "valueConditionId":""
                    },
                    {
                        "fieldId":"LandArea",
                        "value":"SquareFeet",
                        "fieldConditionId":"IsLandArea",
                        "valueConditionId":""
                    },
                    {
                        "fieldId":"RentPrice",
                        "value":0,
                        "fieldConditionId":"ForRent",
                        "valueConditionId":""
                    },
                    {
                        "fieldId":"RentPrice",
                        "value":1500,
                        "fieldConditionId":"ForRent",
                        "valueConditionId":""
                    }
                ]
            },
            "isHomePage":True
        }
        yield scrapy.Request(
            url = 'https://www.centris.ca/property/UpdateQuery',
            method = 'POST',
            body = json.dumps(query),
            headers = {
                'Content-Type': 'application/json',
                'x-requested-with': 'XMLHttpRequest',
                'x-centris-uc': 0,
                'x-centris-uck': uck
            },
            callback = self.update_query
        )
        
    def update_query(self, response):
        yield scrapy.Request(
            url ='https://www.centris.ca/Property/GetInscriptions',
            method = 'POST',
            body = json.dumps(self.position),
            headers = {
                'Content-Type': 'application/json'
            },
            callback = self.parse
        )
    def parse(self,response):
        resp_dict = json.loads(response.body)
        html = resp_dict.get('d').get('Result').get('html')
        sel = Selector(text=html)
        listings = sel.xpath("//div[@class='shell']")
        for listing in listings:
            category = listing.xpath("normalize-space(.//span[@class='category']/div/text())").get()
            bedrooms = listing.xpath(".//div[@class='cac']/text()" ).get()
            bathrooms = listing.xpath(".//div[@class='sdb']/text()" ).get()
            price = listing.xpath(".//div[@class='price']/span[1]/text()").get()
            city = listing.xpath(".//span[@class='address']/div[2]/text()").get()
            url = listing.xpath(".//a[contains(@class,'property-thumbnail-summary-link')]/@href").get()
            yield{
                'category': category,
                'features': f'bedrooms qty: {bedrooms}, bathrooms qty:{bathrooms}',
                'price': price,
                'city': city,
                'url': url
            }
            count = resp_dict.get('d').get('Result').get('count')
            increment_number = resp_dict.get('d').get('Result').get('inscNumberPerPage')
            
            if self.position['startPosition'] <= count:
                self.position['startPosition']+=increment_number
                yield scrapy.Request(
                    url ='https://www.centris.ca/Property/GetInscriptions',
                    method = 'POST',
                    body = json.dumps(self.position),
                    headers = {
                        'Content-Type': 'application/json'
                    },
                    callback = self.parse
                )