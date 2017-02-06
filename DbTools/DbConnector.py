#!/usr/bin/env python3
__author__ = 'papermakkusu'

import MySQLdb


class DbConnector(object):

    def __init__(self,
                 localhost: str="localhost",
                 login: str="admin",
                 password: str="admin",
                 schema: str="ebay",):
        """
        Holds db connection and cursor

        """

        # Connection to host db
        self.connection = None

        # Establish connection to host db
        self.set_connection(localhost, login, password, schema)

    def set_connection(self,
                       localhost: str,
                       login: str,
                       password: str,
                       schema: str,):
        """
        Open database connection. Passes connection to class variable.

        :param localhost: db host
        :type localhost: String
        :param login: db access login
        :type login: String
        :param password: db access password
        :type password: String
        :param schema: db schema
        :type schema: String

        :return: None

        Doctest:
        >>> con = DbConnector("localhost", "admin", "admin", "ebay")
        >>> con.connection.open
        1
        """
        self.connection = MySQLdb.connect(localhost, login, password, schema)

    def close_connection(self, ):
        """
        Close db connection

        :return: None

        Doctest:
        >>> con = DbConnector("localhost", "admin", "admin", "ebay")
        >>> con.connection.open
        1
        >>> con.close_connection()
        >>> con.connection.open
        0
        """

        self.connection.close()


if __name__ == '__main__':
    import doctest
    doctest.testmod()