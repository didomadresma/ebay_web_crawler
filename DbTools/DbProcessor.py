#!/usr/bin/env python3
__author__ = 'papermakkusu'

from DbTools.DbManager import DbManager
from ServiceTools.Switcher import *


class DbProcessor(object):
    """
    Processes requests to db

    """


    def __init__(self,
                 ):
        """
        Initiates DbManager functionality. Contains Db processing information

        :return: None
        """

        self.manager = DbManager()

        # Raw data which db would work with
        self.incoming_data = {}

    def process_db_request(self,
                           commands: dict,
                           ):
        """
        Can process packs of requests to db

        :param commands: A pack of db data of type {DB_COLUMN: DB_VALUE, ...} wrapped in commands
        :type commands: Dictionary
        :return: A pack of results for every executed query. True if Success. False if Fail
        :rtype: Dictionary

        Doctest:
        >>> db_proc = DbProcessor()
        >>> test_dict =  {'insert': {'shipping_info': {'SHIPPING_SERVICE_COST_VALUE': '3.99', 'ONE_DAY_SHIPPING_AVAILABLE': 'false', 'HANDLING_TIME': '4', 'SHIPPING_SERVICE_COST_CURRENCY_ID': 'USD', 'EXPEDITED_SHIPPING': 'false', 'SHIP_TO_LOCATIONS': 'US', 'ITEM_ID': '110149187268', 'SHIPPING_TYPE': 'Flat'}, 'product': {'AUTO_PAY': 'true', 'LOCATION': 'USA', 'GLOBAL_ID': 'EBAY-US', 'PAYMENT_METHOD': 'PayPal', 'COUNTRY': 'US', 'CATEGORY_ID': '0', 'PRODUCT_ID': '116718172', 'VIEW_ITEM_URL': 'http://cgi.sandbox.ebay.com/Nikola-Tesla-Inventions-Researches-1995-Used-Trade-Paper-Paperback-/110149187268', 'ITEM_ID': '110149187268', 'IS_MULTI_VARIATION_LISTING': 'false', 'RETURNS_ACCEPTED': 'true', 'CATEGORY_NAME': None, 'TOP_RATED_LISTING': 'false', 'TITLE': 'Nikola Tesla - Inventions Researches (1995) - Used - Trade Paper (Paperback', 'CONDITION_DISPLAY_NAME': 'Very Good', 'CONDITION_ID': '4000'}, 'selling_status': {'TIME_LEFT': 'P0DT15H46M31S', 'CONVERTED_CURRENT_PRICE_CURRENCY_ID': 'USD', 'CONVERTED_CURRENT_PRICE_VALUE': '3.19', 'CURRENT_PRICE_CURRENCY_ID': 'USD', 'SELLING_STATE': 'Active', 'ITEM_ID': '110149187268', 'CURRENT_PRICE_VALUE': '3.19'}, 'listing_info': {'GIFT': 'false', 'END_TIME': '2016-03-26T10:21:03.000Z', 'ITEM_ID': '110149187268', 'START_TIME': '2014-09-03T10:16:03.000Z', 'FIXED_PRICE': 'NULL', 'BEST_OFFER_ENABLED': 'false', 'BUY_IT_NOW_AVAILABLE': 'false'}}}
        >>> db_proc.process_db_request(test_dict) == {'update': None, 'select': None, 'drop': None, 'insert': True, 'create': None}
        True
        """
        # todo describe request format in header
        # todo Implement advanced search algorithm

        # Container for processing result
        packed_result = {
            'select': None,
            'insert': None,
            'update': None,
            'create': None,
            'drop': None,
        }

        # Parse received request and do corresponding requests to db
        # todo Make possible to process several requests of one type
        for command in commands.keys():
            while Switch(command):
                if case('insert'):
                    packed_result['insert'] = self.multiple_insert(commands[command])

                # Todo implement other options...
                # if case('update'):
                #     packed_result['update'] = self.manager.update_table(table_name=commands[command]['table_name'],
                #                                                         column_names=commands[command]['column_names'],
                #                                                         cell_values=commands[command]['cell_values'],
                #                                                         condition_column_name=commands[command]['condition_column_name'],
                #                                                         condition_cell_value=commands[command]['condition_cell_value'],
                #                                                         )
                # if case('select'):
                #     packed_result['select'] = self.manager.select(table_name=commands[command]['table_name'],
                #                                                   condition_column_name=commands[command]['condition_column_name'],
                #                                                   condition_cell_value=commands[command]['condition_cell_value'],
                #                                                   target_column_name=commands[command]['target_column_name'],
                #                                                   )
                break

        return packed_result

    def multiple_insert(self,
                        data: dict,
                        ):
        """
        Processes multiple inserts into DB

        :param commands: A pack of db data of type {DB_COLUMN: DB_VALUE, ...}
        :type commands: Dictionary
        :return: True if all inserts are successful and False if not
        :rtype: Boolean

        Doctest:
        >>> db_proc = DbProcessor()
        >>> test_dict =  {'shipping_info': {'SHIPPING_SERVICE_COST_VALUE': '3.99', 'ONE_DAY_SHIPPING_AVAILABLE': 'false', 'HANDLING_TIME': '4', 'SHIPPING_SERVICE_COST_CURRENCY_ID': 'USD', 'EXPEDITED_SHIPPING': 'false', 'SHIP_TO_LOCATIONS': 'US', 'ITEM_ID': '110149187268', 'SHIPPING_TYPE': 'Flat'}, 'product': {'AUTO_PAY': 'true', 'LOCATION': 'USA', 'GLOBAL_ID': 'EBAY-US', 'PAYMENT_METHOD': 'PayPal', 'COUNTRY': 'US', 'CATEGORY_ID': '0', 'PRODUCT_ID': '116718172', 'VIEW_ITEM_URL': 'http://cgi.sandbox.ebay.com/Nikola-Tesla-Inventions-Researches-1995-Used-Trade-Paper-Paperback-/110149187268', 'ITEM_ID': '110149187268', 'IS_MULTI_VARIATION_LISTING': 'false', 'RETURNS_ACCEPTED': 'true', 'CATEGORY_NAME': None, 'TOP_RATED_LISTING': 'false', 'TITLE': 'Nikola Tesla - Inventions Researches (1995) - Used - Trade Paper (Paperback', 'CONDITION_DISPLAY_NAME': 'Very Good', 'CONDITION_ID': '4000'}, 'selling_status': {'TIME_LEFT': 'P0DT15H46M31S', 'CONVERTED_CURRENT_PRICE_CURRENCY_ID': 'USD', 'CONVERTED_CURRENT_PRICE_VALUE': '3.19', 'CURRENT_PRICE_CURRENCY_ID': 'USD', 'SELLING_STATE': 'Active', 'ITEM_ID': '110149187268', 'CURRENT_PRICE_VALUE': '3.19'}, 'listing_info': {'GIFT': 'false', 'END_TIME': '2016-03-26T10:21:03.000Z', 'ITEM_ID': '110149187268', 'START_TIME': '2014-09-03T10:16:03.000Z', 'FIXED_PRICE': 'NULL', 'BEST_OFFER_ENABLED': 'false', 'BUY_IT_NOW_AVAILABLE': 'false'}}
        >>> db_proc.multiple_insert(test_dict)
        True
        """

        # Insert in every table over several iteration
        for table in data.keys():
            # Iteratively Insert data in all given tables and quit if something bad happened
            if not self.manager.insert_table(table_name=table, data=data[table]):
                return False
        return True

    def multiple_select(self,):
        """

        :return:
        """
        #todo Implement

    def multiple_update(self, ):
        """

        :return:
        """
        #todo Implement


if __name__ == '__main__':
    import doctest
    doctest.testmod()