import scrapy
import json

from scrapy.selector.unified import _response_from_text


class ListingsSpider(scrapy.Spider):
    name = 'listings'
    allowed_domains = ['www.centris.ca']
    
    def start_requests(self):
        yield scrapy.Request(
            url = 'https://www.centris.ca/UserContext/Lock',
            method = 'POST',
            headers = {
                'content-Type': 'application/json',
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
                'content-Type': 'application/json',
                'x-requested-with': 'XMLHttpRequest',
                'x-centris-uc': 0,
                'x-centris-uck': uck
            },
            callback = self.update_query
        )
        
        
    def update_query(self, response):
        print(response.body)
        
