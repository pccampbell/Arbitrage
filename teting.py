import time

from dotenv import load_dotenv
from ebaysdk.exception import ConnectionError
from ebaysdk.shopping import Connection
import os
from datetime import datetime, timedelta
import sys
import requests

load_dotenv()
API_KEY = os.getenv('api_key')
NOW = datetime.now()
NOW_GMT = datetime.now() + timedelta(hours=6)

class Ebay(object):
    def __init__(self, API_KEY):
        self.api_key = API_KEY
        self.search = search
        self.iaf_token = "v^1.1#i^1#r^0#p^1#I^3#f^0#t^H4sIAAAAAAAAAOVYa2wUVRTuttsiYEsalZeC2+EVhHnvaybdhe0Duk1pF3Yp7fqAedxpx+7OLHPvtF1ISG2UYIxIJEIiKE2MCiJBTQSjgiGaSNCIPwQ0IpQfGGyiiRJSowSd2S5lWwkg3cQm7p/NnHvuud/33XPui+opmfjI5rrNg6WOCYV9PVRPocNBT6YmlhQvKisqnFlcQOU4OPp65vY4e4suVUIhmUjxqwBM6RoEru5kQoN8xhjATEPjdQGqkNeEJIA8kvhoaEUDzxAUnzJ0pEt6AnOFawIYwwmi5KFZhqNYmvOzllW7HjOmBzC/xwcUj4emORqwPi9ttUNogrAGkaAhqz/F0DjN4Iw/Rrl5luZZhvB5qDjmagYGVHXNciEoLJiBy2f6GjlYbw1VgBAYyAqCBcOhZdGmULimtjFWSebECmZ1iCIBmXDkV7UuA1ezkDDBrYeBGW8+akoSgBAjg0MjjAzKh66DuQv4GakVgfLRNOPn/D6fLItsXqRcphtJAd0ah21RZVzJuPJAQypK305RSw3xSSCh7FejFSJc47L/VppCQlVUYASw2qpQaygSwYIRgIBRLSRxIMoiHllVg7M+iWPckiTiokKJboVWsoMMRcpKPGqUal2TVVsw6GrUURWwEIPRujA5ulhOTVqTEVKQjSbXz3tdP7c3bk/o0AyaqF2z5xQkLRFcmc/bqz/cGyFDFU0EhiOMbsjIE8CEVEqVsdGNmTzMpk43DGDtCKV4kuzq6iK6WEI32kiGomiyZUVDVGoHSQHL+tq13g3V23fA1QwVCVg9ocqjdMrC0m3lqQVAa8OCbjfnZams7iNhBUdb/2HI4UyOrIZ8VYcsKjTLcrIHCIzHK1D5qI5gNkFJGwcQhTSeFIwOgFIJQQK4ZOWZmQSGKvOsR2FYvwJw2cspuJtTFFz0yF6cVgCgABBFifP/X4rkTtM8CiQDoLzleV5yPLl8Qz3YUO/vDCnVgBFr9NYOsWp53Fy5PhynGdQcVbjVbWtqGXpRbeBOK+Gm5KsTqqVMzBo/nwLYtT52Eep0iIA8JnpRSU+BiJ5QpfT4mmDWkCOCgdJRkEhYhjGRDKVS4fyt03mh9y+WiLvjnN+96T/Yl27KCtrpOr5Y2f2hFUBIqYS98xCSniR1wTpy2Ka1ULJr3UI9Jt6qdVodV6wtkkNsVXnomElkKBOwUyIMAHXTsE7YRJN98orpHUCz9jJk6IkEMJrpMddyMmkiQUyA8VbUeUhwVRhnGy3tdVNer8fPMWPiJWW20bXjbUnK5zJsG5y+OzxKkyMv9cGCzI/udbxP9TreLXQ4KJKaR8+hKkqKVjuL7p0JVQQIVVAIqLZp1l3VAEQHSKcE1SgscXRN+/D1IznPCH2PU9OHHxImFtGTc14VqIdutBTTU6aVMtZNlfFTbpZmmTg150ark57qvH/Td3PP96/8/NW9i81DcA5J/7V791WqdNjJ4SgucPY6CriDdah+j3h08J4vJ8Vbmwv1yu37K/ejV+rLTr3zwPH5Dx9acuDBxllntlYf+br+s4E3vbGNF9q3XlvwxOCWZxseK/ig6tjO49vKNta1D2w5PHjuBDmld1b3nk/XL9lx7IX+uUepgZe/2P1eX2jvtTXbY2fC5fN27Jmx79vvm3ZdvCL98Mcptmz56bPrTqTLL//04klz6fQzExYXb/noannNtjfODf6899Az36Q733pt1tPnFx6cyn+1r59b2lx/38cnf3/uqUWbfB0vOXc9Wt5zdvGPkaJf+AWX2Ocr3v6tZV1XZ+vO/k9K5g+sU1tmDM6eFDzcMbB29uFS8tzC00zDgQtXrva2XL50+te2+J9tWsXFFUPT9zdd8bii4BEAAA=="

    def fetch(self):
        try:
            api = Connection(iaf_token=self.iaf_token, appid=self.api_key, config_file=None)
            response = api.execute('FindProducts',  {'ProductID': "229082404"})
            print(response.dict()) #.reply.product.productIdentifier.ISBN)

            # api = Connection(iaf_token=self.iaf_token, appid=self.api_key, config_file=None)
            # response = api.execute('FindPopularItems', {'QueryKeywords': 'samsung'})
            # print(response.dict())


            # root = 'https://svcs.ebay.com'
            # endpoint = '/services/marketplacecatalog/ProductService/v1'
            # headers = {
            #     'X-EBAY-API-IAF-TOKEN': 'Bearer ' + OAuth, 'Content-Type': 'application/x-www-form-urlencoded',
            #     'X-EBAY-SOA-RESPONSE-DATA-FORMAT': 'JSON', 'X-EBAY-SOA-SECURITY-APPNAME': self.api_key}
            # params = {
            #     'X-EBAY-SOA-OPERATION-NAME': 'getProductDetails', 'productDetailsRequest': '[0..*]',
            #     'productDetailsRequest.productIdentifier.ePID': 'EPID' + '115115562893'}  # example ePID
            # r = requests.get(root + endpoint, headers=headers, params=params)

        except ConnectionError as e:
            print(e)
            print(e.response.dict())

    def parse(self):
        pass


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start_time = datetime.now()
    search = sys.argv[1]
    e = Ebay(API_KEY)
    e.fetch()
    e.parse()
    end_time = datetime.now()
    print(end_time - start_time)
