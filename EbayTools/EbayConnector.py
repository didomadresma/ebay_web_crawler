#!/usr/bin/env python3
__author__ = 'papermakkusu'

import ebaysdk
from ebaysdk.finding import *
from ebaysdk.finding import Connection as Finding


class EbayConnector(object):
    """

    """

    def __init__(self, ):
        """
        Contains several connections to ebay sandbox servers

        """
        self.find_connection = self.connect_to_finding_server(domain='svcs.ebay.com', config_file='../ebay.yaml')
        self.shop_connection = None
        self.trade_connection = None

    def connect_to_trade_server(self,):
        """

        :return:
        """
        # todo implement

    def connect_to_shopping_server(self,):
        """

        :return:
        """
        # todo implement

    def connect_to_finding_server(self,
                                  domain: str,
                                  config_file: str,
                                  debug: bool=False,
                                  ):
        """
        Establishes connection between application and finding ebay server.

        :param domain: ebay finding sandbox server domain address
        :type domain: String
        :param debug: Debug info output. Set True to turn on.
        :type debug: Boolean
        :param config_file: Config file location
        :type config_file: String
        :return: connection class for finding service
        :rtype: Connection

        Doctest:
        >>> c = EbayConnector()
        >>> type(c.find_connection) == Connection
        True
        """

        # Assign Finding connection to class variable
        return Finding(domain=domain, debug=debug, config_file=config_file, )


if __name__ == '__main__':
    import doctest
    doctest.testmod()