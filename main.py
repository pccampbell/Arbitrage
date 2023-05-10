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
requests.packages.urllib3.disable_warnings()
from multiprocessing import Pool
import random

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

HEADERS_LIST = [
    # Firefox 77 Mac
     {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        # "Referer": "https://www.google.com/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    },
    # Firefox 77 Windows
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        # "Referer": "https://www.google.com/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    },
    # Chrome 83 Mac
    {
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Dest": "document",
        # "Referer": "https://www.google.com/",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"
    },
    # Chrome 83 Windows
    {
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        # "Referer": "https://www.google.com/",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9"
    }
]

def error_handler(_):
    try:
        return _()
    except:
        return ''

def coalesce(*values):
    """Return the first non-None value or None if all values are None"""
    return next((v for v in values if v is not None and v != ''), None)

def parse_item(item):
    # print('parsing item')
    item_list = []
    isbn_10 = None
    isbn_13 = None
    title = error_handler(lambda: item.title)
    price = error_handler(lambda: item.sellingStatus.currentPrice.value)
    shipping = error_handler(lambda: item.shippingInfo.shippingServiceCost.value)
    url = error_handler(lambda: item.viewItemURL)
    #buyitnow = error_handler(lambda: item.listingInfo.buyItNowAvailable)
    endtime = error_handler(lambda: item.listingInfo.endTime).strftime("%Y-%m-%d %H:%M:%S")
    condition = error_handler(lambda: item.condition.conditionDisplayName)

    isbn_10, isbn_13 = grab_isbn(url)

    isbn = coalesce(isbn_13, isbn_10)
    if isbn is None:
        return None

    item_link, buybox_price, book_type, paperback_disp_price, paperback_new_listings_num, paperback_new_listings_from_price, \
    paperback_used_listings_num, paperback_used_listings_from_price, hardcover_disp_price, hardcover_new_listings_num,\
    hardcover_new_listings_from_price, hardcover_used_listings_num, hardcover_used_listings_from_price,\
    best_seller_rank = grab_amazon_data(isbn_10)

    # item_dict = {'title': title, 'price': price, 'isbn': isbn_10, 'shipping': shipping, 'url': url,
    #              'condition': condition, 'endtime': endtime}

    item_dict = {'title': title, 'price': price, 'isbn': isbn_10, 'shipping': shipping, 'url': url,
                 'condition': condition, 'endtime': endtime,
                 'amazon_item_link':item_link,
                 'amazon_buybox_price': buybox_price,
                 'amazon_book_type': book_type,
                 'amazon_paperback_disp_price':paperback_disp_price,
                 'amazon_hardcover_disp_price': hardcover_disp_price,

                 'amazon_paperback_new_listings_num': paperback_new_listings_num,
                 'amazon_paperback_new_listings_from_price': paperback_new_listings_from_price,
                 'amazon_paperback_used_listings_num': paperback_used_listings_num,
                 'amazon_paperback_used_listings_from_price': paperback_used_listings_from_price,

                 'amazon_hardcover_new_listings_num': hardcover_new_listings_num,
                 'amazon_hardcover_new_listings_from_price': hardcover_new_listings_from_price,
                 'amazon_hardcover_used_listings_num': hardcover_used_listings_num,
                 'amazon_hardcover_used_listings_from_price': hardcover_used_listings_from_price,
                 'amazon_best_seller_rank': best_seller_rank
                 }

    print(item_dict)
    # item_list.append(item_dict)
    # print("Appended item")
    # print('item list #: ' + str(len(list)))

    return item_dict


def parse_page(page, item_list):
    st = time.time()
    # Create a dict for each item in the page and add it to the list of dicts, reutn the list
    try:
        page_items = page.reply.searchResult.item
        # page_items = random.sample(page_items, 3)
        # print(len(page_items))
        pool = Pool(1)   #os.cpu_count()) # Pool(4) os.cpu_count()
        parsed_list = pool.map(parse_item, page_items)
        parsed_list = [i for i in parsed_list if i is not None]
        # item_lists = []
        # for item in page_items:
        #     parsed_item = parse_item(item)
        #     if parsed_item is not None:
        #         item_lists.append(parse_item(item))

    finally:
        # print('done')
        pool.close()
        pool.join()

    item_list.extend(parsed_list)

    et = time.time()
    elapsed_time = et - st
    print('Execution time:', elapsed_time, 'seconds')
    # print('party')
    return item_list

    # return item_lists


def call_api(connection, search_term, max_price, page, start, end):
    response = connection.execute('findItemsAdvanced', {'keywords': search_term,
                                                        'paginationInput': {
                                                            'entriesPerPage': 100,
                                                            'pageNumber': page,
                                                        },
                                                        'itemFilter': [
                                                            {'name': 'Condition',
                                                             'value': ['New'],
                                                             },
                                                            {'name': 'MaxPrice',
                                                             'value': max_price,
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
    try:
        connection = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=5432)
        connection.set_session(autocommit=True)
    except(Exception, psycopg2.DatabaseError) as error:
        return error
    return connection


def grab_isbn(link):
    # print(link)
    page = ''
    while page == '':
        try:
            print(link)
            page = requests.get(link, headers=HEADERS)
            proxies = {'http': 'http://brd-customer-hl_49002465-zone-data_center:35192ao3q4f0@zproxy.lum-superproxy.io:22225',
                       'https': 'http://brd-customer-hl_49002465-zone-data_center:35192ao3q4f0@zproxy.lum-superproxy.io:22225'}
            # page = requests.get(link, headers=HEADERS, proxies=proxies, verify=False) #r"C:\Users\PeterCampbell\environments\arbitrage\Arbitrage\bd_cert.crt")
            page = wait_for_response(link)
            break
        except Exception as e:
            print("Connection refused by the server..")
            print(str(e))
            # time.sleep(5)
            continue
    isbn_10 = ''
    isbn_13 = ''
    isbn_10_class = None
    if page.status_code == 200:
        # print('trying')
        isbn_soup = BeautifulSoup(page.text, 'lxml')
        try:
            #Look for ISBN-10 in lower details section
            isbn_10_class = isbn_soup.select('div[class*="isbn-10"]')
            isbn_10 = isbn_10_class[0].find_all(class_="ux-textspans")[1].get_text().strip()
            # print('found lower')
        except:
            isbn_10 = ''

        try:
            # Look for ISBN-13 in lower details section
            isbn_13_class = isbn_soup.select('div[class*="isbn-13"]')
            isbn_13 = isbn_13_class[0].find_all(class_="ux-textspans")[1].get_text().strip()
            # print('found lower')
        except:
            isbn_13 = ''

        #If ISBN-10 not found in lower section look in alternative middle location
        if isbn_10 == '':
            try:
                item_details = isbn_soup.find_all(class_="ux-layout-section--features")
                for ele in item_details:
                    items = ele.find_all(class_="ux-textspans")
                    for i in range(len(items)):
                        #Looks for element text of ISBN-10 knowing the next element will be value (no keywords could be used like above)
                        if items[i].text == 'ISBN-10' or items[i].text == 'ISBN10' or items[i].text == 'ISBN':
                            if len(items[i + 1].text) == 10:
                                isbn_10 = items[i + 1].text
                                # print('found middle')
                                break
                    if isbn != '':
                        break
            except:
                try:
                    item_details = isbn_soup.find_all(class_="ux-layout-section__item")
                    for ele in item_details:
                        items = ele.find_all(class_="ux-textspans")
                        for i in range(len(items)):
                            # Looks for element text of ISBN-10 knowing the next element will be value (no keywords could be used like above)
                            if items[i].text == 'ISBN-10' or items[i].text == 'ISBN10' or items[i].text == 'ISBN':
                                if len(items[i + 1].text) == 10:
                                    isbn_10 = items[i + 1].text
                                    # print('found middle')
                                    break
                        if isbn != '':
                            break
                except:
                    isbn_10 == ''

        if isbn_13 == '':
            try:
                item_details = isbn_soup.find_all(class_="ux-layout-section--features")
                for ele in item_details:
                    items = ele.find_all(class_="ux-textspans")
                    for i in range(len(items)):
                        #Looks for element text of ISBN-13 knowing the next element will be value (no keywords could be used like above)
                        if items[i].text == 'ISBN-13' or items[i].text == 'ISBN13' or items[i].text == 'ISBN':
                            if len(items[i + 1].text) == 13:
                                isbn_13 = items[i + 1].text
                                # print('found middle')
                                break
                    if isbn != '':
                        break
            except:
                try:
                    item_details = isbn_soup.find_all(class_="ux-layout-section__item")
                    for ele in item_details:
                        items = ele.find_all(class_="ux-textspans")
                        for i in range(len(items)):
                            # Looks for element text of ISBN-10 knowing the next element will be value (no keywords could be used like above)
                            if items[i].text == 'ISBN-13' or items[i].text == 'ISBN13' or items[i].text == 'ISBN':
                                if len(items[i + 1].text) == 13:
                                    isbn_13 = items[i + 1].text
                                    # print('found middle')
                                    break
                        if isbn != '':
                            break
                except:
                    isbn_13 == ''
        isbn_soup.decompose()
    else:
        print(page.status_code)

    # isbn_soup.decompose()
    # time.sleep(3)
    print(f'isbn-10:{isbn_10} and isbn-13:{isbn_13}')
    return isbn_10, isbn_13


def wait_for_response(link):
    headers = random.choice(HEADERS_LIST)
    page = requests.get(link, headers=headers)
    attempt = 1
    while page.status_code != 200:
        print(f"Attempt {str(attempt)} failed")
        time.sleep(60*attempt)
        headers = random.choice(HEADERS_LIST)
        page = requests.get(link, headers=headers)
        attempt +=1

    # print(page.status_code)
    return page


def grab_amazon_data(isbn):
    item_link = ''
    buybox_price = ''
    book_type = ''

    paperback_disp_price = ''
    paperback_new_listings_num = ''
    paperback_new_listings_from_price = ''
    paperback_used_listings_num = ''
    paperback_used_listings_from_price = ''

    hardcover_disp_price = ''
    hardcover_new_listings_num = ''
    hardcover_new_listings_from_price = ''
    hardcover_used_listings_num = ''
    hardcover_used_listings_from_price = ''

    best_seller_rank = ''

    base_link_isbn = 'https://www.amazon.com/s?k='
    search_link = base_link_isbn + isbn
    amazon_item_base = 'https://www.amazon.com'

    headers = random.choice(HEADERS_LIST)
    # print(search_link)
    # time.sleep(1)
    page = wait_for_response(search_link)

    amazon_item_base = 'https://www.amazon.com'

    try:
        search_soup = BeautifulSoup(page.text, 'lxml')
        listing = error_handler(lambda: search_soup.find(class_='s-price-instructions-style'))
        book_type = error_handler(lambda: listing.find_all("a")[0].get_text())
        # div = search_soup.find('div', attrs={"data-index": "2"})   #this randomly wasn't working anymore maybe consider trying both
        div = search_soup.find('div', attrs={"data-component-type": "s-search-result"})
        element_link = div.find('a', href=True)['href']
        item_link = amazon_item_base + element_link
        search_soup.decompose()

        print(item_link)
        headers = random.choice(HEADERS_LIST)
        newpage = wait_for_response(item_link)
        new_soup = BeautifulSoup(newpage.text, 'lxml')
    except:
        return item_link, buybox_price, book_type, paperback_disp_price, paperback_new_listings_num,\
               paperback_new_listings_from_price,paperback_used_listings_num, paperback_used_listings_from_price,\
               hardcover_disp_price, hardcover_new_listings_num, hardcover_new_listings_from_price,\
               hardcover_used_listings_num, hardcover_used_listings_from_price, best_seller_rank

    buybox_price = error_handler(lambda: new_soup.find(id="price").get_text().replace('$', '').strip())

    # If empty look in alternate location, if that's empty sometimes re-requesting the page can grab buy box, but this increased proxy cost
    if buybox_price == '':
        buybox_price = error_handler(
            lambda: new_soup.find('div', attrs={"data-feature-name": "corePrice"}).find("span", attrs={
                "class": "a-offscreen"}).get_text().replace('$', '').strip())
        if buybox_price == '':
            # print("buy box not found")
            new_soup.decompose()
            newpage = wait_for_response(item_link)
            new_soup = BeautifulSoup(newpage.text, 'lxml')
            buybox_price = error_handler(lambda: new_soup.find(id="price").get_text().replace('$', '').strip())

    try:
        # Looks through the list of mini buy boxes to grab paperback vs hardcover
        top_list = new_soup.find(id="tmmSwatches").find("ul")
        top_list_elements = top_list.find_all("li")
        for item in top_list_elements:
            if error_handler(lambda: item.find("a").find("span").get_text()) in ['Hardcover', 'Paperback']:
                if error_handler(lambda: item.find("a").find("span").get_text()) == 'Hardcover':
                    hardcover_disp_price = error_handler(
                        lambda: item.find("a").find_all("span")[1].get_text().split('$')[1].split('-')[0].strip())
                elif error_handler(lambda: item.find("a").find("span").get_text()) == 'Paperback':
                    # paperback_disp_price = error_handler(
                    #     lambda: item.find("a").find_all("span")[1].get_text().replace('$', '').split('-')[0].strip())
                    paperback_disp_price = error_handler(
                        lambda: item.find("a").find_all("span")[1].get_text().split('$')[1].split('-')[0].strip())
    except:
        pass

        # This stopped working
        # try:
        #     top_list = new_soup.find_all(class_='olp-link')
        #     if top_list[0][1] == 'Used':
        #         used_listings_num = error_handler(lambda: top_list[0][0])
        #         used_listings_disp_price = error_handler(lambda: top_list[0][3].replace('$', '').strip())
        #         new_listings_num = error_handler(lambda: top_list[1][0])
        #         new_listings_disp_price = error_handler(lambda: top_list[1][3].replace('$', '').strip())
        #     else:
        #         new_listings_num = error_handler(lambda: top_list[0][0])
        #         new_listings_disp_price = error_handler(lambda: top_list[0][3].replace('$', '').strip())
        #         used_listings_num = error_handler(lambda: top_list[1][0])
        #         used_listings_disp_price = error_handler(lambda: top_list[1][3].replace('$', '').strip())
        # except:
        #     pass

        try:
            best_seller_ele = new_soup(text=" Best Sellers Rank: ")
            best_seller_rank = best_seller_ele[0].next_element.strip().split(' ')[0].replace(',', '').replace('#', '')
        except:
            pass

        # print(item_link)
    # print(rent_price, new_buybox_price, new_listings_num, new_listings_disp_price, used_listings_num,
    #       used_listings_disp_price, best_seller_rank)
    try:
        new_soup.decompose()
    except:
        pass

    # print(item_link, buybox_price, book_type, new_listings_num, new_listings_disp_price, used_listings_num, \
    #        used_listings_disp_price, best_seller_rank)
    return item_link, buybox_price, book_type, paperback_disp_price, paperback_new_listings_num, paperback_new_listings_from_price, \
    paperback_used_listings_num, paperback_used_listings_from_price, hardcover_disp_price, hardcover_new_listings_num,\
    hardcover_new_listings_from_price, hardcover_used_listings_num, hardcover_used_listings_from_price,\
    best_seller_rank


def bulk_load_items(db_conn, item_list, table):
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

        with db_conn.cursor() as c:
            c.copy_from(sio, "textbook_search_test", columns=item_df.columns, sep='\t', null="")
            db_conn.commit()
            db_conn.close()

    except psycopg2.Error as e:
        print(str(e))


class Ebay(object):
    def __init__(self, API_KEY, search):
        self.api_key = API_KEY
        self.search = search

    def fetch(self):
        rawdate = NOW_GMT + timedelta(days=6)  # + timedelta(hours=24)
        # rawdate = datetime(2021,12,14,8)
        # consdate = datetime(2021, 12, 14, 8)
        item_list = []
        try:
            api = Connection(appid=self.api_key, config_file=None, siteid="EBAY-US")

            for i in range(1, 320):
                print("starting run: " + str(i))

                # fairly certain the 30 minute windows were to grab a reasonable chunk of results within api limits
                # No more than 10,000 items can be retrieved (10 pages)
                to_datetime = (rawdate + timedelta(days=0)).isoformat() + 'Z'
                from_datetime = (rawdate - timedelta(minutes=30)).isoformat() + 'Z'
                print(from_datetime + '\n' + to_datetime)
                try:
                    # print("calling page 1")
                    max_price = 50
                    response = call_api(api, self.search, max_price, 1, from_datetime, to_datetime)

                    print(f"Total api result {response.reply.paginationOutput.totalEntries}")
                    page = int(response.reply.paginationOutput.pageNumber)
                    pages = int(response.reply.paginationOutput.totalPages)
                    print(f"Total Pages {pages}")

                    # parse page 1
                    item_list = parse_page(response, item_list)
                    print('item list length: ' + str(len(item_list)))

                    print("connecting to DB")
                    conn = connect_db(PG_HOST, 'arbitrage', PG_USER, PG_PASS)
                    print("Connected - loading data to db")
                    bulk_load_items(conn, item_list, "textbook_search_test")
                    item_list = []
                    print("Uploaded list to postgres")

                    if pages >= 2:
                        # print('multipage')
                        for page in range(2, pages + 1):
                            # call new page
                            print(f"calling page: {page}")
                            new_page = call_api(api, self.search, max_price, str(page), from_datetime, to_datetime)
                            # print('succ pulled page: ' + str(new_page.reply.paginationOutput.pageNumber))
                            item_list = parse_page(new_page, item_list)

                            print("connecting to DB")
                            conn = connect_db(PG_HOST, 'arbitrage', PG_USER, PG_PASS)
                            print("Connected - loading data to db")
                            bulk_load_items(conn, item_list, "textbook_search_test")
                            item_list = []
                            print("Uploaded list to postgres")

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
    # conn = connect_db(PG_HOST, 'arbitrage', PG_USER, PG_PASS)
    search = sys.argv[1]
    e = Ebay(API_KEY, search)
    item_list = e.fetch()

    # print("connecting to DB")
    # conn = connect_db(PG_HOST, 'arbitrage', PG_USER, PG_PASS)
    #
    # print("Connected - loading data to db")
    # bulk_load_items(conn, item_list)

    end_time = datetime.now()
    print('script length: ' + str(end_time - start_time))
