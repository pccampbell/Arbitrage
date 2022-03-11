# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import time

from dotenv import load_dotenv
from ebaysdk.exception import ConnectionError
from ebaysdk.finding import Connection
import os
from datetime import datetime, timedelta
import sys
import psycopg2
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool


load_dotenv()
API_KEY = os.getenv('api_key')
NOW = datetime.now()
NOW_GMT = datetime.now() + timedelta(hours=6)

PG_USER = 'postgres'
PG_PASS = os.getenv('pg_pass')
PG_HOST = '10.0.0.26'


def parse_item(item):
    item_list = []
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
    # try:
    #     buyitnow = item.listingInfo.buyItNowAvailable
    # except:
    #     buyitnow = ''
    #     pass
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

    isbn = grab_isbn(url)

    item_dict = {'title': title, 'price': price, 'isbn': isbn, 'shipping': shipping, 'url': url,
                 'condition': condition, 'endtime': endtime}

    # print(item_dict)
    # item_list.append(item_dict)
    # print("Appended item")
    # print('item list #: ' + str(len(list)))

    return item_dict


def parse_page(page, item_list):
    # Create a dict for each item in the page and add it to the list of dicts reutn the list
    try:
        page_items = page.reply.searchResult.item

        pool = Pool(os.cpu_count()-1)  # os.cpu_count()
        parsed_list = pool.map(parse_item, page_items)
        # item_lists = []
        # for item in page_items:
        #     item_lists.append(parse_item(item))

    finally:
        # print('done')
        pool.close()
        pool.join()

    item_list.extend(parsed_list)
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


def connect_db(host, dbname, user, password):
    connection = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=5432)
    return connection


def grab_isbn(link):
    page = ''
    while page == '':
        try:
            page = requests.get(link)
            break
        except:
            print("Connection refused by the server..")
            print("Let me sleep for 5 seconds")
            time.sleep(5)
            continue
    isbn = ''
    if page.status_code == 200:
        try:
            soup = BeautifulSoup(page.content, 'html.parser')
            isbn_class = soup.find(class_="ux-labels-values col-12 ux-labels-values--isbn-10")
            isbn_elements = isbn_class.find_all(class_="ux-textspans")
            isbn = isbn_elements[1].get_text()
        except:
            isbn = ''

    return isbn


def bulk_load_items(db_conn, item_list):
    # create a cursor
    cur = db_conn.cursor()
    # print(item_list)

    # create df from list of dicts
    item_df = pd.DataFrame.from_dict(item_list)
    # execute a statement
    try:
        # Initialize a string buffer
        sio = StringIO()
        sio.write(
            item_df.to_csv(index=False, header=False, sep='\t'))  # Write the Pandas DataFrame as a csv to the buffer
        sio.seek(0)

        with conn.cursor() as c:
            c.copy_from(sio, "ebay", columns=item_df.columns, sep='\t', null="")
            conn.commit()
            conn.close()

    except psycopg2.Error as e:
        print(str(e))


class Ebay(object):
    def __init__(self, API_KEY, search):
        self.api_key = API_KEY
        self.search = search

    def fetch(self):
        rawdate = NOW_GMT + timedelta(days=20) # + timedelta(hours=24)
        # rawdate = datetime(2021,12,14,8)
        # consdate = datetime(2021, 12, 14, 8)
        item_list = []
        try:
            api = Connection(appid=self.api_key, config_file=None, siteid="EBAY-US")

            for i in range(1, 320):
                print("starting run: " + str(i))
                to_datetime = (rawdate + timedelta(days=0)).isoformat() + 'Z'
                from_datetime = (rawdate - timedelta(minutes=30)).isoformat() + 'Z'
                print(from_datetime + '\n' + to_datetime)
                try:
                    # print("calling page 1")
                    response = call_api(api, self.search, 1, from_datetime, to_datetime)

                    print(f"Total api result {response.reply.paginationOutput.totalEntries}")
                    page = int(response.reply.paginationOutput.pageNumber)
                    pages = int(response.reply.paginationOutput.totalPages)
                    # print(f"Total Pages {pages}")

                    # parse page 1
                    item_list = parse_page(response, item_list)
                    # print('item list length: ' + str(len(item_list)))

                    if pages >= 2:
                        # print('multipage')
                        for page in range(2, pages+1):
                            # call new page
                            # print(f"calling page: {page}")
                            new_page = call_api(api, self.search, str(page), from_datetime, to_datetime)
                            # print('succ pulled page: ' + str(new_page.reply.paginationOutput.pageNumber))
                            item_list = parse_page(new_page, item_list)

                except Exception as e:
                    print(str(e))
                    print('No data \n')
                    pass
                i += 1

                rawdate = rawdate + timedelta(minutes=30)
                print('Current List count ' + str(len(item_list)) + '\n\n')
            print('Master List Total: ' + str(len(item_list)))

        except Exception as e:
            print(str(e))
            pass
        return item_list


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start_time = datetime.now()

    search = sys.argv[1]
    e = Ebay(API_KEY, search)
    item_list = e.fetch()

    print("connecting to DB")
    conn = connect_db(PG_HOST, 'arbitrage', PG_USER, PG_PASS)

    print("Connected - loading data to db")
    bulk_load_items(conn, item_list)

    end_time = datetime.now()
    print('script length: ' + str(end_time - start_time))
