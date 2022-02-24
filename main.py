# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import time

from dotenv import load_dotenv
from ebaysdk.exception import ConnectionError
from ebaysdk.finding import Connection
import os
from datetime import datetime, timedelta
import sys

load_dotenv()
API_KEY = os.getenv('api_key')
NOW = datetime.now()
NOW_GMT = datetime.now() + timedelta(hours=6)


class Ebay(object):
    def __init__(self, API_KEY, search):
        self.api_key = API_KEY
        self.search = search

    def fetch(self):
        rawdate = NOW_GMT + timedelta(hours=6)
        # rawdate = datetime(2021,12,14,8)
        # consdate = datetime(2021, 12, 14, 8)
        try:
            api = Connection(appid=self.api_key, config_file=None, siteid="EBAY-US")
            for i in range(1, 100):
                to_datetime = (rawdate + timedelta(days=0)).isoformat() + 'Z'
                from_datetime = (rawdate - timedelta(minutes=30)).isoformat() + 'Z'
                print(i)
                print(from_datetime + '\n' + to_datetime)
                response = api.execute('findItemsAdvanced', {'keywords': search,
                                                             'paginationInput': {
                                                                 'entriesPerPage': 100,
                                                                 'pageNumber': i,
                                                             },
                                                             'itemFilter': [
                                                                 {'name': 'Condition',
                                                                  'value': ['New'],
                                                                  },
                                                                 {'name': 'EndTimeTo',  # on or before
                                                                  'value': to_datetime,
                                                                  },
                                                                 {'name': 'EndTimeFrom',  # on or after
                                                                  'value': from_datetime,
                                                                  },
                                                                 {'name': 'ListingType',
                                                                  'value': 'FixedPrice',
                                                                  }
                                                             ], })

                try:
                    print(f"Total items {response.reply.paginationOutput.totalEntries}\n")
                    # print(f"Page: {response.reply.paginationOutput.pageNumber}")
                    # print(f"Page: {response.reply.itemSearchURL}\n")
                except:
                    pass
                # print(f"Response: {response.reply}")
                # print(f"Items: {len(response.reply.searchResult.item)}")

                for idx, item in enumerate(response.reply.searchResult.item):
                    print(idx)
                    print(f"Title: {item.title}, Price: {item.sellingStatus.currentPrice.value}")
                    try:
                        pid_type = item.productId._type
                        pid = item.productId.value
                        print(f"ID Type : {pid_type}")
                        print(f"ID : {pid}")
                    except:
                        print(f"Response: {item}\n")
                        pass
                    try:
                        if pid_type == 'ReferenceID':
                            response = api.execute('getProductDetails', {'productIdentifier': {
                                                                     'ePID': pid,
                                                                        }
                                                                    }
                                                   )
                            print(response.reply.product.productIdentifier.ISBN)
                        else:
                            pass
                    except:
                        pass
                    #time.sleep(1)
                #    print(f"Buy it now available : {item.listingInfo.buyItNowAvailable}")
                #     print(f"Buy it now available : {item.listingInfo.buyItNowPrice}")
                #     print(f"Buy it now available : {item.listingInfo.listingType}")
                #     print(f"Buy it now available : {item.shippingInfo.shippingServiceCost}")
                #     print(f"Country : {item.country}")
                #     print(f"End time :{item.listingInfo.endTime}")
                #     print(f"URL : {item.viewItemURL}")
                i += 1
                rawdate = rawdate + timedelta(minutes=30)

        except ConnectionError as e:
            print(e)
            print(e.response.dict())

    def parse(self):
        pass


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start_time = datetime.now()
    search = sys.argv[1]
    e = Ebay(API_KEY, search)
    e.fetch()
    e.parse()
    end_time = datetime.now()
    print(end_time - start_time)
