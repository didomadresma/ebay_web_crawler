#!/usr/bin/env python3
__author__ = 'papermakkusu'


class Switch(object):
    """
    Simple imitation of Switch case tool in Python. Used for visual simplification

    """

    value = None

    def __new__(class_, value):
        """
        Assigns given value to class variable on class creation

        :return: Returns True if the assignment was a success
        :rtype: Boolean
        """

        class_.value = value
        return True


def case(*args):
    """
    Matches given value with class value
    Possible utilization:
    #while Switch('b'):
    #    if case('a'):
    #        print("Sad face (T__T)")
    #    if case('b'):
    #        print("Eurica!")
    #>>> Eurica!

    :return: returns True if given argument is in .value field of Switch class
    :rtype: Boolean

    Doctest:
    >>> swi =  Switch('Caramba!')
    >>> case('Pirates!')
    False
    >>> case('Caramba!')
    True
    """

    return any(arg == Switch.value for arg in args)
