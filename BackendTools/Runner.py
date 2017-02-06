#!/usr/bin/env python3
__author__ = 'papermakkusu'

from BackendProcessor import BackendProcessor
processor = BackendProcessor()


def run():
    """
    Run server app. Collect input from command line in specific format and parse eBay for given keywords.
    Contains help.

    Console application enter point

    :return:
    """

    while True:

        # Parse command line arguments and make calls to eBay
        processor.process_command_line_ebay_request()


if __name__ == '__main__':
    run()