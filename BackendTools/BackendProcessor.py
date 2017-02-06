#!/usr/bin/env python3
__author__ = 'papermakkusu'

from ArgParser import ArgParser
from EbayTools.EbayProcessor import EbayProcessor
from EbayTools.EbayResponseParser import EbayResponseParser
from DbTools.DbProcessor import DbProcessor

class BackendProcessor(object):
    """
    Processes data from user input and makes ebay calls wih given parameters.
    Can handle user input from multiple sources: command line, web interface, server requests

    """

    def __init__(self, ):
        """

        :return: None
        """
        self.parser = ArgParser()
        self.ebay_processor = EbayProcessor()
        self.db_processor = DbProcessor()

    def process_web_form_request(self,
                                 text: str):
        """
        """

        # collect keywords from user input
        data = self.parser.parse_web_form_args(text)

        # Make ebay db call if user entered any keywords
        ebay_pack = self.make_ebay_call(data) if data['keywords'] is not None else None
        if not ebay_pack:
            return False

        # insert data into db
        db_result = self.make_db_call(ebay_pack)

        for result in db_result.keys():
            if not result:
                return False
        return ebay_pack


    def process_command_line_ebay_request(self, ):
        """
        Processes the work of user interfaces aka command line app

        :return: True/ False depending on execution result
        :rtype: Boolean

        Doctest:
        # RUN Runner.py to test this method
        """

        # Get user input from command line app
        user_input = input('> ')

        # collect keywords from user input
        data = self.parser.parse_command_line_args(user_input)

        # Make ebay db call if user entered any keywords
        ebay_pack = self.make_ebay_call(data) if data['keywords'] is not None else None
        if not ebay_pack:
            return False

        # insert data into db
        db_result = self.make_db_call(ebay_pack)

        for result in db_result.keys():
            if not result:
                return False
        return True

    def make_ebay_call(self, data: dict):
        """

        :return:
        """

        #TODO implement multiple call options
        return self.ebay_processor.find_item(keywords=data['keywords'])

    def make_db_call(self, parsed_data: dict):
        """

        :return:
        """

        #TODO Implement multiple call options

        return self.db_processor.process_db_request(parsed_data)


if __name__ == '__main__':
    import doctest
    doctest.testmod()