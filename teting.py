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
NOW_GMT = datetime.now() + timedelta(days=29)


def parse_page(page, item_list):
    # Create a dict for each item in the page and add it to the list of dicts reutn the list
    for item in page.reply.searchResult.item:
        try:
            title = item.title
        except:
            title = ''
            pass
        try:
            price = item.sellingStatus.currentPrice.value
        except:
            price = ''
            pass
        try:
            shipping = item.shippingInfo.shippingServiceCost.value
        except:
            shipping = ''
            pass
        try:
            url = item.viewItemURL
        except:
            url = ''
            pass
        try:
            buyitnow = item.listingInfo.buyItNowAvailable
        except:
            buyitnow = ''
            pass
        try:
            endtime = item.listingInfo.endTime
        except:
            endtime = ''
            pass
        try:
            condition = item.condition.conditionDisplayName
        except:
            condition = ''
            pass

        item_dict = {'Title': title, 'Price': price, 'Shipping': shipping, 'URL': url,
                     'Condition': condition, 'BuyItNow': buyitnow, 'EndTime': endtime}

        # print(item_dict)
        item_list.append(item_dict)
        #print('item list #: ' + str(len(list)))

    # print('item list #: ' + str(len(item_list)))

    # print('whats wrong')
    return item_list


def call_api(connection, search_term, page, start, end):
    response = connection.execute('findItemsAdvanced', {'keywords': search_term,
                                                 'paginationInput': {
                                                     'entriesPerPage': 100,
                                                     'pageNumber': page,
                                                 },
                                                 'itemFilter': [
                                                     {'name': 'Condition',
                                                      'value': ['New'],
                                                      },
                                                     {'name': 'LocatedIn',
                                                      'value': ['US'],
                                                      },
                                                     {'name': 'Currency',
                                                      'value': ['USD'],
                                                      },
                                                     {'name': 'EndTimeFrom',  # on or after
                                                      'value': start,
                                                      },
                                                     {'name': 'EndTimeTo',  # on or before
                                                      'value': end,
                                                      },
                                                     {'name': 'ListingType',
                                                      'value': 'FixedPrice',
                                                      }
                                                 ], })
    return response


class Ebay(object):
    def __init__(self, API_KEY, search):
        self.api_key = API_KEY
        self.search = search

    def fetch(self):
        rawdate = NOW_GMT #+ timedelta(hours=6)
        # rawdate = datetime(2021,12,14,8)
        # consdate = datetime(2021, 12, 14, 8)
        try:
            api = Connection(appid=self.api_key, config_file=None, siteid="EBAY-US")

            item_list = []
            for i in range(1, 1000):
                print("starting run: " + str(i))
                to_datetime = (rawdate + timedelta(days=0)).isoformat() + 'Z'
                from_datetime = (rawdate - timedelta(minutes=30)).isoformat() + 'Z'
                print(from_datetime + '\n' + to_datetime)
                try:
                    # print("calling page 1")
                    response = call_api(api, self.search, 1, from_datetime, to_datetime)
                #
                    print(f"Total api result {response.reply.paginationOutput.totalEntries}")
                #     page = int(response.reply.paginationOutput.pageNumber)
                #     pages = int(response.reply.paginationOutput.totalPages)
                #     # print(f"Total Pages {pages}")
                #
                #     # parse page 1
                #     item_list = parse_page(response, item_list)
                #     # print('item list length: ' + str(len(item_list)))
                #
                #     if pages >= 2:
                #         # print('multipage')
                #         for page in range(2, pages+1):
                #             # call new page
                #             # print(f"calling page: {page}")
                #             new_page = call_api(api, self.search, str(page), from_datetime, to_datetime)
                #             # print('succ pulled page: ' + str(new_page.reply.paginationOutput.pageNumber))
                #             item_list = parse_page(new_page, item_list)
                #
                except Exception as e:
                    print(str(e))
                #     print('No data \n')
                #     pass
                i += 1

                rawdate = rawdate + timedelta(minutes=30)
                #print('Current List count ' + str(len(item_list)) + '\n\n')
            #print('Master List Total: ' + str(len(item_list)))

        except Exception as e:
            print(str(e))
            pass

    def parse(self):
        pass

class Database(object):
    def __init__(self, API_KEY, search):
        self.api_key = API_KEY
        self.search = search

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start_time = datetime.now()
    search = sys.argv[1]
    e = Ebay(API_KEY, search)
    e.fetch()
    e.parse()
    end_time = datetime.now()
    print(end_time - start_time)
