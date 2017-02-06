#!/usr/bin/env python3
__author__ = 'papermakkusu'


class EbayRequestParser(object):
    """
    Contains several methods to pack data and send requests to ebay

    """

    @staticmethod
    def get_find_item_advanced_payload(keywords: str,
                                       item_filter: list=None,
                                       affiliate: dict=None,
                                       sortOrder: str=None,
                                       ):
        """
        Packs request data into a proper dictionary

        :param keywords: Unparsed string of keywords from user input
        :type keywords: String
        :param item_filter: List of filters: [{'name': 'Condition', 'value': 'Used'}, ...]
        :type item_filter: List
        :param affiliate: ID of the product distributor for fast find: {'trackingId': 1}
        :type affiliate: Dictionary
        :param sortOrder: SQL parameter for sorting of type: 'CountryDescending'
        :type sortOrder: String
        :return: Dictionary with parameters for advanced search request
        :rtype: Dictionary

        # Doctest verifies that method produces expected amount of keys and values
        Doctest:
        >>> req_parse = EbayRequestParser()
        >>> payload = req_parse.get_find_item_advanced_payload(keywords='tesla', item_filter=[{'name': 'Condition', 'value': 'Used'}, {'name': 'LocatedIn', 'value': 'US'}], affiliate={'trackingId': 1}, sortOrder='CountryDescending')
        >>> check_pl = {'itemFilter': [{'value': 'Used', 'name': 'Condition'}, {'value': 'US', 'name': 'LocatedIn'}], 'sortOrder': 'CountryDescending', 'keywords': 'tesla', 'affiliate': {'trackingId': 1}}
        >>> set(payload.keys()) - set(check_pl.keys())
        set()
        >>> [value for value in payload.keys() if not value in check_pl]
        []
        """

        # Construct request with filters
        request = {
            'keywords': keywords,
        }
        if item_filter is not None: request['itemFilter'] = item_filter
        if affiliate is not None: request['affiliate'] = affiliate
        if sortOrder is not None: request['sortOrder'] = sortOrder

        return request

    @staticmethod
    def get_find_item_by_product_payload(product_id: int,
                                         entries_per_page: int,
                                         ):
        """
        Constructs requests based on product id

        :param product_id: Product ID in eBay database
        :type product_id: Integer
        :param entries_per_page: Number if entries you want to see in one response payload (of all listing)
        :type entries_per_page: Integer
        :return: String with request payload
        :rtype: String

        Doctest:
        >>> req_parse = EbayRequestParser()
        >>> payload = req_parse.get_find_item_by_product_payload(product_id=12, entries_per_page=1)
        >>> check_pl = '<productId type="ReferenceID">12</productId><paginationInput><entriesPerPage>1</entriesPerPage></paginationInput>'
        >>> payload == check_pl
        True
        """

        # todo implement method

        return "".join('<productId type="ReferenceID">' + str(product_id) + '</productId><paginationInput>'
                       '<entriesPerPage>' + str(entries_per_page) + '</entriesPerPage></paginationInput>')
