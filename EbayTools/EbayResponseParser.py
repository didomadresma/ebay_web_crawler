#!/usr/bin/env python3
__author__ = 'papermakkusu'

from DbTools.DbQueryPackersEbay import product_table_columns, listing_info_table_columns, selling_status_table_columns, shipping_info_table_columns
from ServiceTools.ColumnNameFormatter import *
from ServiceTools.Switcher import *
from ServiceTools.DictToObjectConverter import *
import MySQLdb

class EbayResponseParser(object):
    """
    Contains several methods to parse response dictionaries and save results for further processing
    """

    def __init__(self,
                 resp_data,
                 ):
        """
        Contains ebay response data properly parsed for adding to db

        :return: None
        """
        self.raw_data = resp_data.dict()

        # Packet parsed for host db processing
        self.parsed_data = {'insert': {'product':          self.get_product_pack(),
                                       'selling_status':   self.get_selling_status_pack(),
                                       'shipping_info':    self.get_shipping_info_pack(),
                                       'listing_info':     self.get_listing_info_pack(),
                                       },
                            'items_received': resp_data.dict()['searchResult']['_count']}

    def get_product_pack(self, ):
        """
        handles ebay responses to make data pack for Host db Product table

        :return: Packed data for product host db table
        :rtype: Dictionary

        Doctest:
        >>> data = {'itemSearchURL': 'http://shop.sandbox.ebay.com/i.html?LH_ItemCondition=2&LH_LocatedIn=1&_nkw=tesla&_ddo=1&_fls=1&_ipg=100&_pgn=1&_salic=1&_sop=28', 'ack': 'Success', 'searchResult': {'_count': '17', 'item':[{'returnsAccepted': 'true', 'country': 'US', 'productId': {'value': '3161713', '_type': 'ReferenceID'}, 'shippingInfo': {'shipToLocations': 'US', 'shippingType': 'Flat', 'oneDayShippingAvailable': 'false', 'expeditedShipping': 'false', 'shippingServiceCost': {'value': '2.98', '_currencyId': 'USD'}, 'handlingTime': '4'}, 'globalId': 'EBAY-US', 'itemId': '110148795356', 'listingInfo': {'bestOfferEnabled': 'false', 'endTime': '2016-03-24T09:51:41.000Z', 'gift': 'false', 'buyItNowAvailable': 'false', 'startTime': '2014-09-01T09:46:41.000Z', 'listingType': 'FixedPrice'}, 'title': 'Tesla - Bust A Nut (1997) - Used - Compact Disc', 'isMultiVariationListing': 'false', 'condition': {'conditionId': '4000', 'conditionDisplayName': 'Very Good'}, 'primaryCategory': {'categoryName': 'CDs', 'categoryId': '176984'}, 'autoPay': 'true', 'location': 'USA', 'paymentMethod': 'PayPal', 'topRatedListing': 'false', 'sellingStatus': {'convertedCurrentPrice': {'value': '3.49', '_currencyId': 'USD'}, 'currentPrice': {'value': '3.49', '_currencyId': 'USD'}, 'timeLeft': 'P2DT16H0M48S', 'sellingState': 'Active'}, 'viewItemURL': 'http://cgi.sandbox.ebay.com/Tesla-Bust-Nut-1997-Used-Compact-Disc-/110148795356'}]}}
        >>> raw_data = ObjFromDict(data)
        >>> respParser = EbayResponseParser(raw_data)
        >>> respParser.parsed_data == {'insert': {'listing_info': {'FIXED_PRICE': 'NULL', 'BUY_IT_NOW_AVAILABLE': 'false', 'ITEM_ID': '110148795356', 'BEST_OFFER_ENABLED': 'false', 'END_TIME': '2016-03-24T09:51:41.000Z', 'GIFT': 'false', 'START_TIME': '2014-09-01T09:46:41.000Z'}, 'shipping_info': {'ONE_DAY_SHIPPING_AVAILABLE': 'false', 'SHIP_TO_LOCATIONS': 'US', 'HANDLING_TIME': '4', 'EXPEDITED_SHIPPING': 'false', 'ITEM_ID': '110148795356', 'SHIPPING_SERVICE_COST_CURRENCY_ID': 'USD', 'SHIPPING_SERVICE_COST_VALUE': '2.98', 'SHIPPING_TYPE': 'Flat'}, 'selling_status': {'SELLING_STATE': 'Active', 'CURRENT_PRICE_VALUE': '3.49', 'CONVERTED_CURRENT_PRICE_CURRENCY_ID': 'USD', 'ITEM_ID': '110148795356', 'CONVERTED_CURRENT_PRICE_VALUE': '3.49', 'CURRENT_PRICE_CURRENCY_ID': 'USD', 'TIME_LEFT': 'P2DT16H0M48S'}, 'product': {'CATEGORY_NAME': 'CDs', 'TITLE': 'Tesla - Bust A Nut (1997) - Used - Compact Disc', 'GLOBAL_ID': 'EBAY-US', 'PAYMENT_METHOD': 'PayPal', 'COUNTRY': 'US', 'VIEW_ITEM_URL': 'http://cgi.sandbox.ebay.com/Tesla-Bust-Nut-1997-Used-Compact-Disc-/110148795356', 'CONDITION_DISPLAY_NAME': 'Very Good', 'IS_MULTI_VARIATION_LISTING': 'false', 'LOCATION': 'USA', 'CATEGORY_ID': '176984', 'CONDITION_ID': '4000', 'ITEM_ID': '110148795356', 'PRODUCT_ID': '3161713', 'RETURNS_ACCEPTED': 'true', 'TOP_RATED_LISTING': 'false', 'AUTO_PAY': 'true'}}}
        True
        """

        # Parse response dict for product data

        result = {}

        # Iterate over incoming raw data and pick values corresponding to "Product" db table
        for item in self.raw_data['searchResult']['item']:
            for field in item.keys():
                if format_ebay_col_name(field) in product_table_columns:
                    if type(item[field]) == str:
                        if '"' in item[field]: item[field] = item[field].replace('"', '\\"')
                        result[format_ebay_col_name(field)] = item[field]

                # Handle several nested values
                while Switch(format_ebay_col_name(field)):
                    if case('PRODUCT_ID'):
                        result['PRODUCT_ID'] = item[field]['value']
                    if case('PRIMARY_CATEGORY'):
                        result['CATEGORY_ID'] = item[field]['categoryId']
                        result['CATEGORY_NAME'] = item[field]['categoryName']
                    if case('CONDITION'):
                        result['CONDITION_DISPLAY_NAME'] = item[field]['conditionDisplayName']
                        result['CONDITION_ID'] = item[field]['conditionId']
                    break

            # Fill missing values with "NULL"s
            for table_filed in product_table_columns:
                if table_filed not in result.keys(): result[table_filed] = 'NULL'

        return result

    def get_listing_info_pack(self, ):
        """
        Parses responses to make data pack for listing_info table

        :return: Packed data for listing_info host db table
        :rtype: Dictionary

        Doctest:
        >>> data = {'itemSearchURL': 'http://shop.sandbox.ebay.com/i.html?LH_ItemCondition=2&LH_LocatedIn=1&_nkw=tesla&_ddo=1&_fls=1&_ipg=100&_pgn=1&_salic=1&_sop=28', 'ack': 'Success', 'searchResult': {'_count': '17', 'item':[{'returnsAccepted': 'true', 'country': 'US', 'productId': {'value': '3161713', '_type': 'ReferenceID'}, 'shippingInfo': {'shipToLocations': 'US', 'shippingType': 'Flat', 'oneDayShippingAvailable': 'false', 'expeditedShipping': 'false', 'shippingServiceCost': {'value': '2.98', '_currencyId': 'USD'}, 'handlingTime': '4'}, 'globalId': 'EBAY-US', 'itemId': '110148795356', 'listingInfo': {'bestOfferEnabled': 'false', 'endTime': '2016-03-24T09:51:41.000Z', 'gift': 'false', 'buyItNowAvailable': 'false', 'startTime': '2014-09-01T09:46:41.000Z', 'listingType': 'FixedPrice'}, 'title': 'Tesla - Bust A Nut (1997) - Used - Compact Disc', 'isMultiVariationListing': 'false', 'condition': {'conditionId': '4000', 'conditionDisplayName': 'Very Good'}, 'primaryCategory': {'categoryName': 'CDs', 'categoryId': '176984'}, 'autoPay': 'true', 'location': 'USA', 'paymentMethod': 'PayPal', 'topRatedListing': 'false', 'sellingStatus': {'convertedCurrentPrice': {'value': '3.49', '_currencyId': 'USD'}, 'currentPrice': {'value': '3.49', '_currencyId': 'USD'}, 'timeLeft': 'P2DT16H0M48S', 'sellingState': 'Active'}, 'viewItemURL': 'http://cgi.sandbox.ebay.com/Tesla-Bust-Nut-1997-Used-Compact-Disc-/110148795356'}]}}
        >>> raw_data = ObjFromDict(data)
        >>> respParser = EbayResponseParser(raw_data)
        >>> respParser.parsed_data == {'insert': {'shipping_info': {'ONE_DAY_SHIPPING_AVAILABLE': 'false', 'SHIPPING_SERVICE_COST_VALUE': '2.98', 'ITEM_ID': '110148795356', 'SHIP_TO_LOCATIONS': 'US', 'EXPEDITED_SHIPPING': 'false', 'SHIPPING_SERVICE_COST_CURRENCY_ID': 'USD', 'SHIPPING_TYPE': 'Flat', 'HANDLING_TIME': '4'}, 'listing_info': {'START_TIME': '2014-09-01T09:46:41.000Z', 'FIXED_PRICE': 'NULL', 'END_TIME': '2016-03-24T09:51:41.000Z', 'BEST_OFFER_ENABLED': 'false', 'GIFT': 'false', 'ITEM_ID': '110148795356', 'BUY_IT_NOW_AVAILABLE': 'false'}, 'selling_status': {'CONVERTED_CURRENT_PRICE_VALUE': '3.49', 'TIME_LEFT': 'P2DT16H0M48S', 'ITEM_ID': '110148795356', 'CONVERTED_CURRENT_PRICE_CURRENCY_ID': 'USD', 'CURRENT_PRICE_VALUE': '3.49', 'SELLING_STATE': 'Active', 'CURRENT_PRICE_CURRENCY_ID': 'USD'}, 'product': {'RETURNS_ACCEPTED': 'true', 'IS_MULTI_VARIATION_LISTING': 'false', 'AUTO_PAY': 'true', 'VIEW_ITEM_URL': 'http://cgi.sandbox.ebay.com/Tesla-Bust-Nut-1997-Used-Compact-Disc-/110148795356', 'TITLE': 'Tesla - Bust A Nut (1997) - Used - Compact Disc', 'LOCATION': 'USA', 'GLOBAL_ID': 'EBAY-US', 'COUNTRY': 'US', 'CONDITION_ID': '4000', 'CATEGORY_ID': '176984', 'PAYMENT_METHOD': 'PayPal', 'ITEM_ID': '110148795356', 'CONDITION_DISPLAY_NAME': 'Very Good', 'PRODUCT_ID': '3161713', 'CATEGORY_NAME': 'CDs', 'TOP_RATED_LISTING': 'false'}}}
        True
        """

        result = {}

        # Iterate over incoming raw data and pick values corresponding to "Listing Info" db table
        for item in self.raw_data['searchResult']['item']:
            for field in item['listingInfo'].keys():
                if format_ebay_col_name(field) in listing_info_table_columns:
                    if type(item['listingInfo'][field]) == str:
                        result[format_ebay_col_name(field)] = item['listingInfo'][field]

            # Fill missing values with "NULL"s
            for table_filed in listing_info_table_columns:
                if table_filed not in result.keys(): result[table_filed] = 'NULL'

            # Set Item ID
            result['ITEM_ID'] = item['itemId']

        return result

    def get_selling_status_pack(self, ):
        """
        Parses responses to make data pack for selling_status table

        :return: Packed data for selling_status host db table
        :rtype: Dictionary

        Doctest:
        >>> data = {'itemSearchURL': 'http://shop.sandbox.ebay.com/i.html?LH_ItemCondition=2&LH_LocatedIn=1&_nkw=tesla&_ddo=1&_fls=1&_ipg=100&_pgn=1&_salic=1&_sop=28', 'ack': 'Success', 'searchResult': {'_count': '17', 'item':[{'returnsAccepted': 'true', 'country': 'US', 'productId': {'value': '3161713', '_type': 'ReferenceID'}, 'shippingInfo': {'shipToLocations': 'US', 'shippingType': 'Flat', 'oneDayShippingAvailable': 'false', 'expeditedShipping': 'false', 'shippingServiceCost': {'value': '2.98', '_currencyId': 'USD'}, 'handlingTime': '4'}, 'globalId': 'EBAY-US', 'itemId': '110148795356', 'listingInfo': {'bestOfferEnabled': 'false', 'endTime': '2016-03-24T09:51:41.000Z', 'gift': 'false', 'buyItNowAvailable': 'false', 'startTime': '2014-09-01T09:46:41.000Z', 'listingType': 'FixedPrice'}, 'title': 'Tesla - Bust A Nut (1997) - Used - Compact Disc', 'isMultiVariationListing': 'false', 'condition': {'conditionId': '4000', 'conditionDisplayName': 'Very Good'}, 'primaryCategory': {'categoryName': 'CDs', 'categoryId': '176984'}, 'autoPay': 'true', 'location': 'USA', 'paymentMethod': 'PayPal', 'topRatedListing': 'false', 'sellingStatus': {'convertedCurrentPrice': {'value': '3.49', '_currencyId': 'USD'}, 'currentPrice': {'value': '3.49', '_currencyId': 'USD'}, 'timeLeft': 'P2DT16H0M48S', 'sellingState': 'Active'}, 'viewItemURL': 'http://cgi.sandbox.ebay.com/Tesla-Bust-Nut-1997-Used-Compact-Disc-/110148795356'}]}}
        >>> raw_data = ObjFromDict(data)
        >>> respParser = EbayResponseParser(raw_data)
        >>> respParser.parsed_data == {'insert': {'listing_info': {'BEST_OFFER_ENABLED': 'false', 'ITEM_ID': '110148795356', 'BUY_IT_NOW_AVAILABLE': 'false', 'FIXED_PRICE': 'NULL', 'END_TIME': '2016-03-24T09:51:41.000Z', 'GIFT': 'false', 'START_TIME': '2014-09-01T09:46:41.000Z'}, 'selling_status': {'ITEM_ID': '110148795356', 'CURRENT_PRICE_CURRENCY_ID': 'USD', 'CURRENT_PRICE_VALUE': '3.49', 'CONVERTED_CURRENT_PRICE_VALUE': '3.49', 'TIME_LEFT': 'P2DT16H0M48S', 'SELLING_STATE': 'Active', 'CONVERTED_CURRENT_PRICE_CURRENCY_ID': 'USD'}, 'product': {'RETURNS_ACCEPTED': 'true', 'CATEGORY_ID': '176984', 'CONDITION_ID': '4000', 'TOP_RATED_LISTING': 'false', 'TITLE': 'Tesla - Bust A Nut (1997) - Used - Compact Disc', 'VIEW_ITEM_URL': 'http://cgi.sandbox.ebay.com/Tesla-Bust-Nut-1997-Used-Compact-Disc-/110148795356', 'IS_MULTI_VARIATION_LISTING': 'false', 'PAYMENT_METHOD': 'PayPal', 'ITEM_ID': '110148795356', 'CONDITION_DISPLAY_NAME': 'Very Good', 'COUNTRY': 'US', 'AUTO_PAY': 'true', 'PRODUCT_ID': '3161713', 'CATEGORY_NAME': 'CDs', 'LOCATION': 'USA', 'GLOBAL_ID': 'EBAY-US'}, 'shipping_info': {'SHIPPING_SERVICE_COST_CURRENCY_ID': 'USD', 'SHIP_TO_LOCATIONS': 'US', 'ONE_DAY_SHIPPING_AVAILABLE': 'false', 'ITEM_ID': '110148795356', 'SHIPPING_TYPE': 'Flat', 'SHIPPING_SERVICE_COST_VALUE': '2.98', 'HANDLING_TIME': '4', 'EXPEDITED_SHIPPING': 'false'}}}
        True
        """

        result = {}

        # Iterate over incoming raw data and pick values corresponding to "Selling_status" db table
        for item in self.raw_data['searchResult']['item']:
            for field in item['sellingStatus'].keys():
                if format_ebay_col_name(field) in selling_status_table_columns:
                    if type(item['sellingStatus'][field]) == str:
                        result[format_ebay_col_name(field)] = item['sellingStatus'][field]

                # Handle several nested values
                while Switch(format_ebay_col_name(field)):
                    if case('CONVERTED_CURRENT_PRICE'):
                        result['CONVERTED_CURRENT_PRICE_CURRENCY_ID'] = item['sellingStatus'][field]['_currencyId']
                        result['CONVERTED_CURRENT_PRICE_VALUE'] = item['sellingStatus'][field]['value']
                    if case('CURRENT_PRICE'):
                        result['CURRENT_PRICE_CURRENCY_ID'] = item['sellingStatus'][field]['_currencyId']
                        result['CURRENT_PRICE_VALUE'] = item['sellingStatus'][field]['value']
                    break

            # Fill missing values with "NULL"s
            for table_filed in selling_status_table_columns:
                if table_filed not in result.keys(): result[table_filed] = 'NULL'

            # Set Item ID
            result['ITEM_ID'] = item['itemId']

        return result

    def get_shipping_info_pack(self, ):
        """
        Parses responses to make data pack for shipping_info table

        :return: Packed data for shipping_info db table
        :rtype: Dictionary

        Doctest:
        >>> data = {'itemSearchURL': 'http://shop.sandbox.ebay.com/i.html?LH_ItemCondition=2&LH_LocatedIn=1&_nkw=tesla&_ddo=1&_fls=1&_ipg=100&_pgn=1&_salic=1&_sop=28', 'ack': 'Success', 'searchResult': {'_count': '17', 'item':[{'returnsAccepted': 'true', 'country': 'US', 'productId': {'value': '3161713', '_type': 'ReferenceID'}, 'shippingInfo': {'shipToLocations': 'US', 'shippingType': 'Flat', 'oneDayShippingAvailable': 'false', 'expeditedShipping': 'false', 'shippingServiceCost': {'value': '2.98', '_currencyId': 'USD'}, 'handlingTime': '4'}, 'globalId': 'EBAY-US', 'itemId': '110148795356', 'listingInfo': {'bestOfferEnabled': 'false', 'endTime': '2016-03-24T09:51:41.000Z', 'gift': 'false', 'buyItNowAvailable': 'false', 'startTime': '2014-09-01T09:46:41.000Z', 'listingType': 'FixedPrice'}, 'title': 'Tesla - Bust A Nut (1997) - Used - Compact Disc', 'isMultiVariationListing': 'false', 'condition': {'conditionId': '4000', 'conditionDisplayName': 'Very Good'}, 'primaryCategory': {'categoryName': 'CDs', 'categoryId': '176984'}, 'autoPay': 'true', 'location': 'USA', 'paymentMethod': 'PayPal', 'topRatedListing': 'false', 'sellingStatus': {'convertedCurrentPrice': {'value': '3.49', '_currencyId': 'USD'}, 'currentPrice': {'value': '3.49', '_currencyId': 'USD'}, 'timeLeft': 'P2DT16H0M48S', 'sellingState': 'Active'}, 'viewItemURL': 'http://cgi.sandbox.ebay.com/Tesla-Bust-Nut-1997-Used-Compact-Disc-/110148795356'}]}}
        >>> raw_data = ObjFromDict(data)
        >>> respParser = EbayResponseParser(raw_data)
        >>> respParser.parsed_data == {'insert': {'listing_info': {'GIFT': 'false', 'BUY_IT_NOW_AVAILABLE': 'false', 'BEST_OFFER_ENABLED': 'false', 'ITEM_ID': '110148795356', 'START_TIME': '2014-09-01T09:46:41.000Z', 'FIXED_PRICE': 'NULL', 'END_TIME': '2016-03-24T09:51:41.000Z'}, 'shipping_info': {'SHIP_TO_LOCATIONS': 'US', 'SHIPPING_SERVICE_COST_CURRENCY_ID': 'USD', 'EXPEDITED_SHIPPING': 'false', 'HANDLING_TIME': '4', 'ITEM_ID': '110148795356', 'ONE_DAY_SHIPPING_AVAILABLE': 'false', 'SHIPPING_TYPE': 'Flat', 'SHIPPING_SERVICE_COST_VALUE': '2.98'}, 'selling_status': {'CURRENT_PRICE_CURRENCY_ID': 'USD', 'TIME_LEFT': 'P2DT16H0M48S', 'CONVERTED_CURRENT_PRICE_VALUE': '3.49', 'SELLING_STATE': 'Active', 'ITEM_ID': '110148795356', 'CURRENT_PRICE_VALUE': '3.49', 'CONVERTED_CURRENT_PRICE_CURRENCY_ID': 'USD'}, 'product': {'CONDITION_DISPLAY_NAME': 'Very Good', 'TITLE': 'Tesla - Bust A Nut (1997) - Used - Compact Disc', 'COUNTRY': 'US', 'CATEGORY_NAME': 'CDs', 'CONDITION_ID': '4000', 'PRODUCT_ID': '3161713', 'IS_MULTI_VARIATION_LISTING': 'false', 'AUTO_PAY': 'true', 'ITEM_ID': '110148795356', 'GLOBAL_ID': 'EBAY-US', 'VIEW_ITEM_URL': 'http://cgi.sandbox.ebay.com/Tesla-Bust-Nut-1997-Used-Compact-Disc-/110148795356', 'RETURNS_ACCEPTED': 'true', 'PAYMENT_METHOD': 'PayPal', 'CATEGORY_ID': '176984', 'LOCATION': 'USA', 'TOP_RATED_LISTING': 'false'}}}
        True
        """

        result = {}

        # Iterate over incoming raw data and pick values corresponding to "Selling_status" db table
        for item in self.raw_data['searchResult']['item']:
            for field in item['shippingInfo'].keys():
                if format_ebay_col_name(field) in shipping_info_table_columns:
                    if type(item['shippingInfo'][field]) == str:
                        result[format_ebay_col_name(field)] = item['shippingInfo'][field]

                # Handle several nested values
                while Switch(format_ebay_col_name(field)):
                    if case('SHIPPING_SERVICE_COST'):
                        result['SHIPPING_SERVICE_COST_CURRENCY_ID'] = item['shippingInfo'][field]['_currencyId']
                        result['SHIPPING_SERVICE_COST_VALUE'] = item['shippingInfo'][field]['value']
                    break

            # Fill missing values with "NULL"s
            for table_filed in shipping_info_table_columns:
                if table_filed not in result.keys(): result[table_filed] = 'NULL'

            # Set Item ID
            result['ITEM_ID'] = item['itemId']

        return result

    def check_result(self, ):
        """
        Verifies response payload

        :return:
        """
        pass


if __name__ == '__main__':
    import doctest
    doctest.testmod()