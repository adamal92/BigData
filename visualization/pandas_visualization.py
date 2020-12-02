from typing import Any, Union

import pandas
import matplotlib.pyplot as plt
import numpy
from pandas import Series, DataFrame
from pandas.io.parsers import TextFileReader

"""
matrix: list = [(1, 2), (3, 4)]
dataframe: pandas.DataFrame = pandas.DataFrame(data=matrix)
print(dataframe)


# var: Union[Union[TextFileReader, Series, DataFrame, None], Any] = pandas.read_csv()

var2: Union[Union[TextFileReader, Series, DataFrame, None], Any] =\
    Union[Union[TextFileReader, Series, DataFrame, None], Any](matrix)

var2.loan_purpose_name.value_counts().plot(kind='barh')

"""

matrix: list = [(1, 2, 3), (3, 4, 5)]

def matrix_to_dict(matrix: list):
    """
    converts a matrix to a dictionary
    :param matrix :type list: the matrix to be converted
    :return :type dict: a dictionary that contains all the rows as keys & values
    """
    dictionary: dict = {}
    for row in matrix:
        dictionary[str(row)] = row
    return dictionary

print(matrix_to_dict(matrix))


def visualize_matrix(matrix: list):
    # pass_dict: dict = {'list1': (11, 12, 13), 'list2': [21, 22, 23]}
    # dataframe: pandas.DataFrame = pandas.DataFrame(data=pass_dict, index=[1, 2, 3])
    """
    visualizes a matrix as a column graph
    :param matrix :type list: the matrix to be visualized
    :return:
    """
    """
    The resulted graph:
    
    values
    3                                   |
    2       |                           |
    1       |               |           |
       (key, index)  (key, index)  (key, index)
    """
    pass_dict: dict = matrix_to_dict(matrix=matrix)
    dataframe: pandas.DataFrame = pandas.DataFrame(data=pass_dict)
    print()
    print(dataframe)
    df_lists = dataframe[list(pass_dict.keys())].unstack().apply(pandas.Series)
    df_lists.plot.bar(rot=0, cmap=plt.cm.jet, fontsize=8, width=0.7, figsize=(8, 4))
    plt.show()


visualize_matrix(matrix)
