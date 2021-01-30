from enum import Enum, unique


@unique
class PrintForm(Enum):
    PRINT_BRACKETS = 0  # {
    PRINT_DICT = 1  # dict {
    PRINT_ARROWS = 2  # <class 'dict'>
    NO_PRINT = 3  # don't print

    def __str__(self):
        return "PRINT_BRACKETS: {\nPRINT_DICT: dict {\nPRINT_ARROWS: <class 'dict'>\nNO_PRINT: nothing"

    def get_value(self):
        return self.name, self.value

    @classmethod
    def print_options(cls):
        print(cls.__str__(PrintForm.PRINT_DICT))


class DataTypesHandler:
    """
    Handles common structured data manipulations
    """
    # static
    PRINT_BRACKETS = 0  # {
    PRINT_DICT = 1  # dict {
    PRINT_ARROWS = 2  # <class 'dict'> {{

    @staticmethod
    def print_dict(dictionary: dict):
        """
        prints recursively a dictionary in a nice format
        :param dictionary :type dict: the dictionary to be printed
        :return :type None
        """
        for key in dictionary.keys():
            value = dictionary[key]
            if type(value) is dict:
                print("%s: {" % key)
                DataTypesHandler.print_dict(value)
                print("}")
            elif type(value) is list:
                DataTypesHandler.print_2D_matrix(dictionary[key])
            else:
                print(f"{key}: {value}")

    @staticmethod
    def print_2D_matrix(table: list) -> None:
        """
        Prints any table (matrix) to the console in a nice format
        :param table :type list: the table to be printed
        :return :type None
        """
        for row in table:
            print_string: str = "| "
            if type(row) in (list, dict):
                for value in row:
                    print_string += str(value) + " | "
            else: print_string += str(row) + " | "
            print(print_string)

    @staticmethod
    def matrix_to_dict(matrix: list, schema: (list, tuple)=None) -> dict:
        """
        converts a matrix to a dictionary
        :param schema :type list[str]: a list of strings containing the column names
        :param matrix :type list: the matrix to be converted
        :return :type dict: a dictionary that contains all the rows as keys & values
        """
        dictionary: dict = {}

        if schema:
            rowCount: int = 0
            for row in matrix:
                for i in range(0, len(schema)):
                    dictionary[f"{schema[i]} row {str(rowCount)} item {i}"] = row[i]
                # for colomn in schema:
                #     dictionary[f"{colomn} row {str(rowCount)}"] = row.pop()
                rowCount += 1
        else:
            for row in matrix:
                dictionary[str(row)] = row

        return dictionary

    @staticmethod
    def dict_to_matrix(dictionary: dict) -> list:
        """
        converts a dictionary to a matrix
        :param dictionary :type dict: the dictionary to be converted
        :return :type list:  a matrix that contains all the keys & values as rows & columns
        """
        matrix: list = []
        for key in dictionary:
            matrix.append([key, dictionary[key]])
        return matrix

    @staticmethod
    def print_data_recursively(data, tab: str="", print_dict: int=PRINT_ARROWS):
        """
        Print any of the supported types (list, dict, tuple) to the console in a nice format
        :param data :type one of the supported types:
        :param tab :type str: a string containing the tubs that are added at the start of every line
        :param print_dict :type int, one of the PRINT constants:
        determines in which way the dictionaries would be printed
        :return :type None
        """
        # check for supported types
        supported_types: tuple = (list, dict, tuple)
        if not type(data) in supported_types:
            raise Exception(f'data must be a list, a dictionary or a tuple. got {type(data)}')

        # handle lists & tuples
        elif type(data) in (list, tuple):
            print_string: str = tab+"[ "
            for item in data:
                if type(item) in supported_types:
                    DataTypesHandler.print_data_recursively(data=item, tab=tab, print_dict=print_dict)
                else:
                    print_string += str(item) + " | "
            print(print_string + "]")

        # handle dictionaries
        elif type(data) is dict:
            if print_dict == DataTypesHandler.PRINT_BRACKETS: print(tab+"{")
            elif print_dict == DataTypesHandler.PRINT_DICT: print(tab+"%s {" % str(type(data)).split("'")[1])
            elif print_dict == DataTypesHandler.PRINT_ARROWS: print(tab+"%s {" % type(data))
            elif print_dict == PrintForm.NO_PRINT: return
            else: print(tab+"%s {" % type(data))

            for key in data.keys():
                value = data[key]
                if type(value) in supported_types:
                    print(tab+f"    {key}: ")
                    DataTypesHandler.print_data_recursively(data=value, tab=tab+"    ", print_dict=print_dict)
                else:
                    print(tab+f"    {key}: {value}")
            print(tab+"}")
