#!/usr/bin/env python3
__author__ = 'papermakkusu'

from itertools import zip_longest
from ServiceTools.Switcher import *


# All tables columns
product_table_columns =         ('ITEM_ID', 'PRODUCT_ID', 'GLOBAL_ID', 'TITLE', 'COUNTRY', 'RETURNS_ACCEPTED',
                                 'IS_MULTI_VARIATION_LISTING', 'CONDITION_ID', 'CONDITION_DISPLAY_NAME',
                                 'CATEGORY_ID', 'CATEGORY_NAME', 'AUTO_PAY', 'LOCATION', 'PAYMENT_METHOD',
                                 'TOP_RATED_LISTING', 'VIEW_ITEM_URL')
listing_info_table_columns =    ('ITEM_ID', 'BEST_OFFER_ENABLED', 'END_TIME', 'GIFT', 'BUY_IT_NOW_AVAILABLE',
                                 'START_TIME', 'FIXED_PRICE')
selling_status_table_columns =  ('ITEM_ID', 'CONVERTED_CURRENT_PRICE_CURRENCY_ID',
                                 'CONVERTED_CURRENT_PRICE_VALUE', 'CURRENT_PRICE_CURRENCY_ID',
                                 'CURRENT_PRICE_VALUE', 'TIME_LEFT', 'SELLING_STATE')
shipping_info_table_columns =   ('ITEM_ID', 'SHIP_TO_LOCATIONS', 'SHIPPING_TYPE',
                                 'ONE_DAY_SHIPPING_AVAILABLE', 'EXPEDITED_SHIPPING',
                                 'SHIPPING_SERVICE_COST_CURRENCY_ID', 'SHIPPING_SERVICE_COST_VALUE', 'HANDLING_TIME')


def pack_for_create_table(tables: tuple):
    """
    Packs data for "create table" query

    :param tables: A set of tables to create
    :type tables: Tuple
    :return: Returns a composed sql query for table creation
    :rtype: String

    Doctest:
    >>> tables = ('selling_status', )
    >>> create_query = pack_for_create_table(tables)
    >>> create_query[:43]
    'CREATE TABLE IF NOT EXISTS selling_status ('
    """

    all_tables = {'product':       """CREATE TABLE IF NOT EXISTS product (
                                          ITEM_ID                              CHAR(50)         NOT NULL UNIQUE,
                                          PRODUCT_ID                           CHAR(50),
                                          GLOBAL_ID                            CHAR(50),
                                          TITLE                                TEXT,
                                          COUNTRY                              CHAR(50),
                                          RETURNS_ACCEPTED                     CHAR(50),
                                          IS_MULTI_VARIATION_LISTING           CHAR(50),
                                          CONDITION_ID                         CHAR(50),
                                          CONDITION_DISPLAY_NAME               CHAR(50),
                                          CATEGORY_ID                          CHAR(50),
                                          CATEGORY_NAME                        CHAR(100),
                                          AUTO_PAY                             CHAR(50),
                                          LOCATION                             CHAR(50),
                                          PAYMENT_METHOD                       CHAR(50),
                                          TOP_RATED_LISTING                    CHAR(50),
                                          VIEW_ITEM_URL                        TEXT
                                          )""",
                  'listing_info':      """CREATE TABLE IF NOT EXISTS listing_info (
                                          ITEM_ID                              CHAR(50)         NOT NULL UNIQUE,
                                          BEST_OFFER_ENABLED                   CHAR(20),
                                          END_TIME                             CHAR(50),
                                          GIFT                                 CHAR(20),
                                          BUY_IT_NOW_AVAILABLE                 CHAR(20),
                                          START_TIME                           CHAR(50),
                                          FIXED_PRICE                          CHAR(20)
                                          )""",
                  'selling_status':    """CREATE TABLE IF NOT EXISTS selling_status (
                                          ITEM_ID                              CHAR(50)         NOT NULL UNIQUE,
                                          CONVERTED_CURRENT_PRICE_CURRENCY_ID  CHAR(20),
                                          CONVERTED_CURRENT_PRICE_VALUE        CHAR(50),
                                          CURRENT_PRICE_CURRENCY_ID            CHAR(20),
                                          CURRENT_PRICE_VALUE                  CHAR(50),
                                          TIME_LEFT                            CHAR(50),
                                          SELLING_STATE                        CHAR(20)
                                          )""",
                  'shipping_info':     """CREATE TABLE IF NOT EXISTS shipping_info (
                                          ITEM_ID                              CHAR(50)         NOT NULL UNIQUE,
                                          SHIP_TO_LOCATIONS                    CHAR(20),
                                          SHIPPING_TYPE                        CHAR(100),
                                          ONE_DAY_SHIPPING_AVAILABLE           CHAR(20),
                                          EXPEDITED_SHIPPING                   CHAR(20),
                                          SHIPPING_SERVICE_COST_CURRENCY_ID    CHAR(50),
                                          SHIPPING_SERVICE_COST_VALUE          CHAR(50),
                                          HANDLING_TIME                        CHAR(50)
                                          )"""
                  }

    # If table was not specified, put all tables in line
    tables = all_tables.keys() if tables is None else tables

    return ';'.join(["{}".format(all_tables[i]) for i in tables])


def pack_for_insert(table_name: str,
                    data: dict):
    """
    Properly packs incoming data in insert sql query

    :param table: A table to insert into
    :type table: String
    :return: Returns a composed sql query for insertion into table
    :rtype: String

    Doctest:
    >>> data={'CONVERTED_CURRENT_PRICE_VALUE': '19.95', 'CURRENT_PRICE_VALUE': '20.25', 'TIME_LEFT': 'P3DT4H58M27S', 'ITEM_ID': '110100720611', 'SELLING_STATE': 'Active', 'CONVERTED_CURRENT_PRICE_CURRENCY_ID': 'USD', 'CURRENT_PRICE_CURRENCY_ID': 'USD'}
    >>> pack_for_insert(table_name='selling_status', data=data)
    "REPLACE INTO selling_status (ITEM_ID, CONVERTED_CURRENT_PRICE_CURRENCY_ID, CONVERTED_CURRENT_PRICE_VALUE, CURRENT_PRICE_CURRENCY_ID, CURRENT_PRICE_VALUE, TIME_LEFT, SELLING_STATE) VALUES ('110100720611', 'USD', '19.95', 'USD', '20.25', 'P3DT4H58M27S', 'Active');"
    """

    # Decide which table apply insert query to
    columns_list = product_table_columns    # "product" by default
    while Switch(table_name):
        if case('listing_info'):
            columns_list = listing_info_table_columns
        if case('selling_status'):
            columns_list = selling_status_table_columns
        if case('shipping_info'):
            columns_list = shipping_info_table_columns
        break

    # Compose condition string for Update statement from several set operations separated by commas
    columns_string = r"{}".format(tuple([i for i in columns_list])).replace("'", "")

    # Init service variables
    values_string, count = r"", 0

    # Update Statement string with commas separating set values
    for column in columns_list:
        for key, value in data.items():
            if column == key:
                values_string = values_string + r'"{}"{}'.format(value, (", " if count<len(data)-1 else ''))
                count += 1

    return r"REPLACE INTO {} {} VALUES ({});".format(table_name, columns_string, values_string)


def pack_for_update(table_name: str,
                    for_update: dict,
                    condition: dict,
                    ):
    """
    Properly packs incoming data and conditions in update sql query

    :param table_name: Name of table to update
    :type table_name: String
    :param for_update: set of column names and values to update of type: {DB_COLUMN: DB_VALUE, ...}
    :type for_update: Dictionary
    :param condition: set of column name and value serving as condition of type: {DB_COLUMN: DB_VALUE, ...}
    :type condition: Dictionary
    :return: Returns a composed sql query for table update
    :rtype: String

    Doctest:
    >>> for_update={'CONVERTED_CURRENT_PRICE_VALUE': '100.95', 'CURRENT_PRICE_VALUE': '30.25'}
    >>> condition={'ITEM_ID': '110100720611'}
    >>> pack_for_update(table_name='selling_status', for_update=for_update, condition=condition)
    "UPDATE selling_status SET CONVERTED_CURRENT_PRICE_VALUE='100.95', CURRENT_PRICE_VALUE='30.25' WHERE ITEM_ID='110100720611'"
    """

    column_names, cell_values = for_update.keys(), for_update.values()

    # todo Make possible to work with several conditions
    # Currently works with one condition only
    # Take first argument for condition column name and value
    condition_column_name, condition_cell_value = next(iter(condition.keys())), next(iter(condition.values()))

    # Matches column names with cell values. On output looks like: ('name1=value1, name2=value2, ...')
    update_list = tuple([("{}='{}'".format(name, value)) for (name, value) in zip_longest(column_names, cell_values)])

    # Compose condition string for Update statement from several set operations separated by commas
    separator = ", " if len(update_list) > 1 else ""    # Decide on whether we need commas in statement
    condition_string, count = "", 0                     # Init service variables
    for i in update_list:                               # Update Statement string with commas separating set values
        condition_string = condition_string + r'{}{}'.format(i, (separator if count<len(update_list)-1 else ''))
        count+=1

    return r'UPDATE {} SET {} WHERE {}="{}"'.format(table_name, "{}".format(condition_string),
                                                   condition_column_name, condition_cell_value)


def pack_for_select(table_name: str,
                    condition_column_name: str=None,
                    condition_cell_value: str=None,
                    target_column_name: str=None,
                    ):
    """
    Packs data for select query

    :param table_name: Name of table to select from
    :type table_name: String
    :param condition_column_name: Name of conditional column
    :type condition_column_name: String
    :param condition_cell_value: Value of conditional column
    :type condition_cell_value: String
    :param target_column_name: Name of the column which value we want to get. Replaced with "*" if not specified
    :type target_column_name: String
    :return: Returns a composed sql select query
    :rtype: String

    Doctest:
    >>> select_query = pack_for_select(table_name='product', target_column_name='TITLE', condition_column_name='ITEM_ID', condition_cell_value='111222')
    >>> print(select_query)
    SELECT TITLE FROM product WHERE ITEM_ID='111222';
    """

    # Name of the column to select. If no parameters given it would be "*"
    target = "*" if target_column_name is None else target_column_name

    # Optional "WHERE" condition constructor. If no parameters given it would be empty
    if condition_column_name and condition_cell_value is not None:
        condition = ' WHERE {0:s}="{1:s}"'.format(condition_column_name, condition_cell_value)
    else:
        condition = ""

    return "SELECT {0:s} FROM {1:s}{2:s};".format(target, table_name, condition)


def pack_for_drop(tables: tuple,
                  ):
    """
    Packs data for "drop table" query

    :param tables: A set of tables to drop
    :type tables: Tuple
    :return: Returns a composed sql drop query
    :rtype: String

    Doctest:
    >>> pack_for_drop(('product', ))
    'DROP TABLE IF EXISTS product'
    """

    return ';'.join(["DROP TABLE IF EXISTS {}".format(i) for i in tables])


if __name__ == '__main__':
    import doctest
    doctest.testmod()
