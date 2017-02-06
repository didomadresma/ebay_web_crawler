#!/usr/bin/env python3
__author__ = 'papermakkusu'


def format_ebay_col_name(name: str, ):
    """
    Splits string inside string with _ before capital letters.
    Then makes all letters uppercase.

    :param name: raw string
    :type name: String
    :return: Splitted and capitalized string. Mainly used for Host DB column names.
    :rtype: String

    Doctest:
        >>> name = 'viewItemURL'
        >>> format_ebay_col_name(name)
        'VIEW_ITEM_URL'
    """

    new_name = ''
    is_first_letter = True

    # Fix eBay exceptions in naming
    name = name.replace("URL", "Url")

    # Iterate over every letter in column name
    for letter in name:
        # Put '_" before every capital letter which is not the first
        new_name += '_' + letter if not is_first_letter and letter.isupper() else letter
        is_first_letter = False

    # Make all letters uppercase and return
    return new_name.upper()


if __name__ == '__main__':
    import doctest
    doctest.testmod()