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

HEADERS = ({'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36',
            'Accept-Language': 'en-US, en;q=0.5'})


def parse_item(item):
    # print('parsing item')
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

    rent_price, new_buybox_price, new_listings_num, new_listings_disp_price, used_listings_num, \
    used_listings_disp_price, best_seller_rank = grab_amazon_data(isbn)

    item_dict = {'title': title, 'price': price, 'isbn': isbn, 'shipping': shipping, 'url': url,
                 'condition': condition, 'endtime': endtime, 'amazon_rentprice': rent_price,
                 'amazon_new_buybox_price': new_buybox_price, 'amazon_newlistings_num': new_listings_num,
                 'amazon_new_listings_disp_price': new_listings_disp_price,
                 'amazon_used_listings_num': used_listings_num,
                 'amazon_used_listings_disp_price': used_listings_disp_price,
                 'amazon_best_seller_rank': best_seller_rank}

    # print(item_dict)
    # item_list.append(item_dict)
    # print("Appended item")
    # print('item list #: ' + str(len(list)))

    return item_dict


def parse_page(page, item_list):
    # Create a dict for each item in the page and add it to the list of dicts reutn the list
    try:
        page_items = page.reply.searchResult.item

        pool = Pool(4)  # os.cpu_count()
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
            page = requests.get(link, headers=HEADERS)
            break
        except:
            print("Connection refused by the server..")
            print("Let me sleep for 5 seconds")
            time.sleep(5)
            continue
    isbn = ''
    if page.status_code == 200:
        try:
            isbn_soup = BeautifulSoup(page.content, 'html.parser')
            isbn_class = isbn_soup.find(class_="ux-labels-values col-12 ux-labels-values--isbn-10")
            isbn_elements = isbn_class.find_all(class_="ux-textspans")
            isbn = isbn_elements[1].get_text()
        except:
            isbn = ''

    isbn_soup.decompose()
    return isbn


def grab_amazon_data(isbn):
    base_link_isbn = 'https://www.amazon.com/s?k='

    search_link = base_link_isbn + isbn

    print(search_link)
    time.sleep(1)
    page = requests.get(search_link, headers=HEADERS)

    amazon_item_base = 'https://www.amazon.com'

    rent_price = ''
    new_buybox_price = ''
    new_listings_num = ''
    new_listings_disp_price = ''
    used_listings_num = ''
    used_listings_disp_price = ''
    best_seller_rank = ''

    if page.status_code == 200:
        try:
            search_soup = BeautifulSoup(page.content, 'html.parser')
            div = search_soup.find('div', attrs={"data-index": "2"})
            element_link = div.find('a', href=True)['href']
            item_link = amazon_item_base + element_link
            search_soup.decompose()

            newpage = requests.get(item_link, headers=HEADERS)
            new_soup = BeautifulSoup(newpage.content, 'html.parser')
        except:
            return rent_price, new_buybox_price, new_listings_num, new_listings_disp_price, used_listings_num, \
                   used_listings_disp_price, best_seller_rank

        try:
            rent_price = new_soup.find('span', id="rentPrice").get_text().replace('$', '').strip()
        except:
            pass

        try:
            new_buybox_price = new_soup.find('span', id="newBuyBoxPrice").get_text().replace('$', '').strip()
        except:
            pass

        try:
            new_listings_element = new_soup.find('span',
                                                 attrs={"data-show-all-offers-display": """{"condition":"new"}"""})
            new_listings_spans = new_listings_element.find_all('span')
            new_listings_num = new_listings_spans[0].get_text().split(' ')[0]
            new_listings_disp_price = new_listings_element.find(class_="a-color-price").get_text().replace('$',
                                                                                                           '').strip()
        except:
            pass

        try:
            used_listings_element = new_soup.find('span',
                                                  attrs={"data-show-all-offers-display": """{"condition":"used"}"""})
            used_listings_spans = used_listings_element.find_all('span')
            used_listings_num = used_listings_spans[0].get_text().split(' ')[0]
            used_listings_disp_price = used_listings_element.find(class_="a-color-price").get_text().replace('$',
                                                                                                             '').strip()
        except:
            pass

        try:
            best_seller_ele = new_soup(text=" Best Sellers Rank: ")
            best_seller_rank = best_seller_ele[0].next_element.strip().split(' ')[0].replace(',', '').replace('#', '')
        except:
            pass
    else:
        print('amazon page not reached')
    # print(rent_price, new_buybox_price, new_listings_num, new_listings_disp_price, used_listings_num,
    #       used_listings_disp_price, best_seller_rank)
    try:
        new_soup.decompose()
    except:
        pass

    return rent_price, new_buybox_price, new_listings_num, new_listings_disp_price, used_listings_num, \
           used_listings_disp_price, best_seller_rank


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
        rawdate = NOW_GMT + timedelta(days=20)  # + timedelta(hours=24)
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
                        for page in range(2, pages + 1):
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
