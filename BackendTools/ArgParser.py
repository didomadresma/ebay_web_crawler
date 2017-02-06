#!/usr/bin/env python3
__author__ = 'papermakkusu'

from sys import exit, path
from os.path import abspath, dirname, join
path.append(join(dirname(abspath(__file__)), abspath('../')))
from ServiceTools.Switcher import *
from copy import deepcopy


class ArgParser(object):
    """
    Parser for User input arguments

    """

    def __init__(self, ):
        """

        :return:
        """

        self.parsed_args = None

    def parse_command_line_args(self,
                   user_input: str,
                   ):
        """
        Parses user input and properly packs it for further processing

        :param user_input: User input from command line
        :type user_input: String
        :return: Parsed user input packed in dictionary
        :rtype: Dictionary

        Doctest:
        >>> ap = ArgParser()
        >>> ap.parse_command_line_args("-k tesla")
        {'keywords': 'tesla'}
        """

        # Split user input into tokens
        tokens = user_input.split()

        # Split user input into tokens and separate commands from arguments
        commands, args = deepcopy(tokens), deepcopy(tokens)
        del commands[1::2]
        del args[0::2]

        # Parse commands and arguments into a dictionary
        parsed_input = {'keywords': None, }

        # Put parsed arguments into dictionary along with values
        for i in range(len(commands)):
            while Switch(commands[i]):
                # Given keywords
                if case('-k'):
                    parsed_input['keywords'] = args[i]
                # Write query result to db
                if case('-w'):
                    parsed_input['write'] = args[i]
                # Exit application on user demand
                if case('-s'):
                    parsed_input['select'] = args[i]
                if case('-exit'):
                    exit()
                break

        return parsed_input

    def parse_web_form_args(self,
                            user_input: str,
                            ):
        """
        Parses user input from web form and properly packs it for further processing

        """

        # Parse commands and arguments into a dictionary
        parsed_input = {'keywords': user_input,}

        return parsed_input


if __name__ == '__main__':
    import doctest
    doctest.testmod()