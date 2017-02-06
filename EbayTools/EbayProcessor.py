#!/usr/bin/env python3
__author__ = 'papermakkusu'

from EbayTools.EbayConnector import EbayConnector
from EbayTools.EbayRequestParser import EbayRequestParser
from EbayTools.EbayResponseParser import EbayResponseParser
from ServiceTools.Logger import logger as log

class EbayProcessor(object):
    """
    Contains several methods to parse response dictionaries and save results for further processing
    """

    def __init__(self,
                 ):
        """
        Performs constant eBay db processing

        :return: None
        """

        self.find_connection = EbayConnector().find_connection
        self.req_parser = EbayRequestParser
        self.resp_parser = EbayResponseParser

    def find_item(self,
                  keywords: str,
                  item_filter: list=None,
                  affiliate: dict=None,
                  sortOrder: str=None,
                  product_id: int=None,
                  entries_per_page: int=None,
                  ):
        """
        Chooses the optimal algorithm for item search based on incoming data.
        Then triggers corresponding search mechanism.

        :param keywords: text keys passed from web interface of command line. This argument is mandatory.
        :type keywords: String
        :param item_filter: Not implemented
        :type item_filter: List
        :param affiliate: Not implemented
        :type affiliate: Dictionary
        :param sortOrder: Not implemented
        :type sortOrder: String
        :param product_id: Not implemented
        :type product_id: Integer
        :param entries_per_page: Not implemented
        :type entries_per_page: Integer
        :return: Data from eBay parsed into dict under "insert" for further processing and insertion into host DB
        :rtype: Dictionary

        Doctest:
        >>> c = EbayProcessor()
        >>> response = c.find_item(keywords='tesla', item_filter=[{'name': 'Condition', 'value': 'Used'}, {'name': 'LocatedIn', 'value': 'US'}], affiliate={'trackingId': 1}, sortOrder='CountryDescending')
        >>> response['insert']['product']['COUNTRY'] == 'US'
        True
        >>> response = c.find_item(keywords=('audi',))
        >>> 'AUDI' in response['insert']['product']['TITLE']
        True
        """

        # todo Implement advanced arguments parsing
        #retrun response package from ebay req based on given keywords
        if keywords is not None:
            # Get data from ebay
            raw_data = self.find_item_advanced(keywords, item_filter, affiliate, sortOrder)

            if int(raw_data.dict()['searchResult']['_count']) <= 0:
                log.warning("No Items found for keywords: {}".format(keywords))
                return False

            # Return parsed data
            return self.resp_parser(raw_data).parsed_data
        else:
            #todo remove when search is fully implemented
            raise ValueError("'keyword' argument value missing")

        #Todo Implement full list of search option

    def find_item_advanced(self,
                           keywords: str,
                           item_filter: list=None,
                           affiliate: dict=None,
                           sortOrder: str=None,
                           ):
        """
        Search items by given keywords. Filters and Sorters could be applied to search result

        :param keywords: Keywords the search based on
        :type keywords: String
        :param item_filter: list of conditions for search results filtering
        :type item_filter: list
        :param affiliate: additional optional filtering condition
        :type affiliate: dictionary
        :param affiliate: sets the sorting order preference for search results
        :type affiliate: String
        :return: container consisting of several parsed dictionaries with response results
        :rtype: data pointer

        Doctest:
        >>> c = EbayProcessor()
        >>> response = c.find_item_advanced(keywords='tesla', item_filter=[{'name': 'Condition', 'value': 'Used'}, {'name': 'LocatedIn', 'value': 'US'}], affiliate={'trackingId': 1}, sortOrder='CountryDescending')
        >>> 'Success' == response.dict()['ack']
        True
        >>> response = c.find_item_advanced(keywords=('tesla', 'audi', 'volvo',))
        >>> 'Success' == response.dict()['ack']
        True
        """
        try:

            # Construct request with filters
            request = self.req_parser.get_find_item_advanced_payload(keywords, item_filter, affiliate, sortOrder)

            return self.find_connection.execute("findItemsAdvanced", request)

        except ConnectionError as e:
            print(e)

    def find_item_by_product(self,
                             product_id: int,
                             entries_per_page: int,
                             ):
        """
        Search items by given product id

        :param product_id: Product identification number in eBay db
        :type product_id: Integer
        :param entries_per_page: number of entries to output per page
        :type entries_per_page: Integer
        :return: container consisting of several parsed dictionaries with response results
        :rtype: data pointer

        Doctest:
        >>> c = EbayProcessor()
        >>> response = c.find_item_by_product(product_id=53039031, entries_per_page=1)
        >>> 'Success' == response.dict()['ack']
        True
        """
        try:
            # Construct request based in product id
            request = self.req_parser.get_find_item_by_product_payload(product_id, entries_per_page)

            return self.find_connection.execute('findItemsByProduct', request)

        except ConnectionError as e:
            print(e)

if __name__ == '__main__':
    import doctest
    doctest.testmod()