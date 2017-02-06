__author__ = 'papermakkusu'


class ObjFromDict(object):
    """
    Generalized object class

    """

    def __init__(self,
                 data: dict):
        """
        Wraps dictionaries into Object class

        :param data: Dict to be wrapped in Object class
        :type data: Dictionary
        :return:
        """

        self.data = data

    def dict(self, ):
        """
        Returns .data field of the object

        :return: .data field of a class
        :rtype: Dictionary

        Doctest:
        >>> dict = {'a': 1}
        >>> obj_from_dict = ObjFromDict(dict)
        >>> obj_from_dict.dict()
        {'a': 1}
        """
        return self.data


if __name__ == '__main__':
    import doctest
    doctest.testmod()