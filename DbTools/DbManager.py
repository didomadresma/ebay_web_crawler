#!/usr/bin/env python3
__author__ = 'papermakkusu'

from DbTools.DbConnector import DbConnector
from ServiceTools.Logger import logger as log
from DbTools.DbQueryPackersEbay import *
import warnings
import MySQLdb


class DbManager(object):

    def __init__(self, ):
        """
        Holds db info

        :return:
        """
        try:
            # Create connection to db
            self.connection = DbConnector().connection

        except MySQLdb.MySQLError:
            log.info('Exception while connecting to host db: ', )

        # Create all tables if they do not exist
        self.create_table()

    def create_table(self,
                     tables: tuple=None,
                     ):
        """
        Create required table in database
        If the table is not specified, method will try to create all available tables

        :param tables: table names should be Strings packed iin a tuple, like this: ('product, ...)
        :type tables: Tuple
        :return: Return True in case query returned success
        :rtype: Boolean

        Doctest:
        >>> db_manager = DbManager()
        >>> data={'CONVERTED_CURRENT_PRICE_VALUE': '19.95', 'CURRENT_PRICE_VALUE': '19.95', 'TIME_LEFT': 'P3DT4H58M27S', 'ITEM_ID': '110100720611', 'SELLING_STATE': 'Active', 'CONVERTED_CURRENT_PRICE_CURRENCY_ID': 'USD', 'CURRENT_PRICE_CURRENCY_ID': 'USD'}
        >>> db_manager.insert_table(table_name='selling_status', data=data)
        True
        >>> response = db_manager.select(table_name='selling_status', target_column_name='CONVERTED_CURRENT_PRICE_VALUE', condition_column_name='ITEM_ID', condition_cell_value='110100720611')
        >>> for item in response: print(item)
        ('19.95',)
        >>> db_manager.drop_tables(tables=('selling_status', ))
        True
        """

        # Pack an SQL "Create table" request
        create_table_query = pack_for_create_table(tables)
        # Set the cursor for work
        cursor = self.connection.cursor()

        # Send request on creation of specified table
        try:
            # Catch warnings to not let them in standard output
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                # Execute "Create table" query.
                # Generator does not support buffer interface, so we do it in several iterations
                for i in create_table_query.split(";"):
                    cursor.execute(i)
                # Commit changes to db
                self.connection.commit()
                # Clean cursor
                cursor.fetchall()
                return True
        except MySQLdb.IntegrityError:
            log.exception("Failed to create tables {}, exception: ".format(tables))
        finally:
            # Free cursor
            cursor.close()

    def drop_tables(self,
                    tables: tuple=None,
                    ):
        """
        Drops requested tables

        :param tables: Several names of tables to drop
        :type tables: Tuple
        :return: Returns True if query was a success
        :rtype: Boolean

        Doctest:
        >>> db_manager = DbManager()
        >>> data={'CURRENT_PRICE_VALUE': '9.39', 'SELLING_STATE': 'Active', 'CONVERTED_CURRENT_PRICE_VALUE': '9.39', 'CURRENT_PRICE_CURRENCY_ID': 'USD', 'ITEM_ID': '110133199135', 'CONVERTED_CURRENT_PRICE_CURRENCY_ID': 'USD', 'TIME_LEFT': 'P11DT4H4M16S'}
        >>> db_manager.insert_table(table_name='selling_status', data=data)
        True
        >>> response = db_manager.select(table_name='selling_status', target_column_name='CONVERTED_CURRENT_PRICE_VALUE', condition_column_name='ITEM_ID', condition_cell_value='110133199135')
        >>> for item in response: print(item)
        ('9.39',)
        >>> db_manager.drop_tables(tables=('selling_status', ))
        True
        """

        # Set the cursor for work
        cursor = self.connection.cursor()

        try:
            # Get packed data for drop query
            drop_pack = pack_for_drop(tables)
            # Drop table if listed in "table_names" and already exist.
            cursor.execute(drop_pack)
            # Commit changes to db
            self.connection.commit()
            # Clean cursor
            cursor.fetchall()
            return True
        except MySQLdb.IntegrityError:
            log.exception("Could not drop tables")
        finally:
            # Free cursor
            cursor.close()

    def insert_table(self,
                          data: dict,
                          table_name: str='product',
                          ):
        """
        Insert into table with data parsed for insertion into db table

        :param data: Contains pairs {DB_COLUMN: DB_VALUE, ...}
        :type data: Dictionary
        :param table_name: Name of the table to insert into
        :type table_name: String
        :return: Returns True if query was a success
        :rtype: Boolean

        Doctest:
        >>> db_manager = DbManager()
        >>> data={'CONVERTED_CURRENT_PRICE_VALUE': '19.95', 'CURRENT_PRICE_VALUE': '19.95', 'TIME_LEFT': 'P3DT4H58M27S', 'ITEM_ID': '110100720611', 'SELLING_STATE': 'Active', 'CONVERTED_CURRENT_PRICE_CURRENCY_ID': 'USD', 'CURRENT_PRICE_CURRENCY_ID': 'USD'}
        >>> db_manager.insert_table(table_name='selling_status', data=data)
        True
        >>> response = db_manager.select(table_name='selling_status', target_column_name='CURRENT_PRICE_VALUE', condition_column_name='ITEM_ID', condition_cell_value='110100720611')
        >>> for item in response: print(item)
        ('19.95',)
        >>> data={'CONVERTED_CURRENT_PRICE_VALUE': '19.95', 'CURRENT_PRICE_VALUE': '20.25', 'TIME_LEFT': 'P3DT4H58M27S', 'ITEM_ID': '110100720611', 'SELLING_STATE': 'Active', 'CONVERTED_CURRENT_PRICE_CURRENCY_ID': 'USD', 'CURRENT_PRICE_CURRENCY_ID': 'USD'}
        >>> db_manager.insert_table(table_name='selling_status', data=data)
        True
        >>> response = db_manager.select(table_name='selling_status', target_column_name='CURRENT_PRICE_VALUE', condition_column_name='ITEM_ID', condition_cell_value='110100720611')
        >>> for item in response: print(item)
        ('20.25',)
        >>> db_manager.drop_tables(tables=('selling_status', ))
        True
        """

        # Pack data for insert operation
        insert_query = pack_for_insert(table_name, data)

        # Set the cursor for work
        cursor = self.connection.cursor()

        try:
            # Execute Insert query
            cursor.execute(insert_query)
            # Commit changes to db
            self.connection.commit()
            # Clean cursor
            cursor.fetchall()
            return True
        except MySQLdb.IntegrityError:
            log.exception('Exception while inserting into table {}:'.format(table_name))
        finally:
            # Free cursor
            cursor.close()

    def update_table(self,
                     table_name: str,
                     for_update: dict,
                     condition: dict,
                     ):
        """
        Update table with data parsed for insertion into db table

        :param table_name: Name of the table to update
        :type table_name: String
        :param for_update: Pairs of type {DB_COLUMN: DB_VALUE, ...} to be updated
        :type for_update: Dictionary
        :param conditions: Pairs of type {DB_COLUMN: DB_VALUE, ...} to serve as conditions for update
        :type conditions: Dictionary
        :return: Returns True if query was a success
        :rtype: Boolean

        Doctest:
        >>> db_manager = DbManager()
        >>> data={'CONVERTED_CURRENT_PRICE_VALUE': '19.95', 'CURRENT_PRICE_VALUE': '19.95', 'TIME_LEFT': 'P3DT4H58M27S', 'ITEM_ID': '110100720611', 'SELLING_STATE': 'Active', 'CONVERTED_CURRENT_PRICE_CURRENCY_ID': 'USD', 'CURRENT_PRICE_CURRENCY_ID': 'USD'}
        >>> db_manager.insert_table(table_name='selling_status', data=data)
        True
        >>> response = db_manager.select(table_name='selling_status', target_column_name='CURRENT_PRICE_VALUE', condition_column_name='ITEM_ID', condition_cell_value='110100720611')
        >>> for item in response: print(item)
        ('19.95',)
        >>> db_manager.update_table(table_name='selling_status', for_update={'CURRENT_PRICE_VALUE': '100.15'}, condition={'ITEM_ID': '110100720611'})
        True
        >>> response = db_manager.select(table_name='selling_status', target_column_name='CURRENT_PRICE_VALUE', condition_column_name='ITEM_ID', condition_cell_value='110100720611')
        >>> for item in response: print(item)
        ('100.15',)
        >>> db_manager.drop_tables(tables=('selling_status', ))
        True
        """

        # Pack conditional data for update query
        update_query = pack_for_update(table_name, for_update, condition)

        # Set the cursor for work
        cursor = self.connection.cursor()

        try:
            # Execute "Update table" query
            cursor.execute(update_query)
            # Commit changes to db
            self.connection.commit()
            # Clean cursor
            cursor.fetchall()
            return True
        except MySQLdb.IntegrityError:
            log.exception('Exception while updating table {}:'.format(table_name))
        finally:
            # Free cursor
            cursor.close()

    def select(self,
               table_name: str,
               condition_column_name: str=None,
               condition_cell_value: str=None,
               target_column_name: str=None,
               ):
        """
        Executes "Select" SQL query with given parameters

        :param table_name: Name of the requested table
        :type table_name: String
        :param target_column_name: Name of the column which value would be returned
        :type target_column_name: String
        :param condition_column_name: Conditional column name
        :type condition_column_name: String
        :param condition_cell_value: Conditional column value
        :type condition_cell_value: String
        :return: Data resulting from Select SQL query
        :rtype: Tuple

        Doctest:
        >>> db_manager = DbManager()
        >>> data={'CONVERTED_CURRENT_PRICE_VALUE': '19.95', 'CURRENT_PRICE_VALUE': '19.95', 'TIME_LEFT': 'P3DT4H58M27S', 'ITEM_ID': '110100720611', 'SELLING_STATE': 'Active', 'CONVERTED_CURRENT_PRICE_CURRENCY_ID': 'USD', 'CURRENT_PRICE_CURRENCY_ID': 'USD'}
        >>> db_manager.insert_table(table_name='selling_status', data=data)
        True
        >>> response = db_manager.select(table_name='selling_status', target_column_name='CURRENT_PRICE_VALUE', condition_column_name='ITEM_ID', condition_cell_value='110100720611')
        >>> for item in response: print(item)
        ('19.95',)
        >>> db_manager.drop_tables(tables=('selling_status', ))
        True
        """

        # Pack data for "select" query
        select_query = pack_for_select(table_name, condition_column_name, condition_cell_value, target_column_name)

        # Set the cursor for work
        cursor = self.connection.cursor()
        try:
            # Execute "select" query
            cursor.execute(select_query)
            # Clean cursor and return result of "select" query
            return cursor.fetchall()
        except MySQLdb.IntegrityError:
            log.exception('Exception while selecting from table {}:'.format(table_name))
        finally:
            # Free cursor
            cursor.close()


if __name__ == '__main__':
    import doctest
    doctest.testmod()
