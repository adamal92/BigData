# imports
import pandas, logging
import matplotlib.pyplot


class VisualizationHandler:
    """

    """
    @staticmethod
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

    @staticmethod
    def visualize_matrix(matrix: list):
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
        try:
            pass_dict: dict = VisualizationHandler.matrix_to_dict(matrix=matrix)
            dataframe: pandas.DataFrame = pandas.DataFrame(data=pass_dict)
            df_lists = dataframe[list(pass_dict.keys())].unstack().apply(pandas.Series)
            df_lists.plot.bar(rot=0, cmap=matplotlib.pyplot.cm.jet, fontsize=8, width=0.7, figsize=(8, 4))
            matplotlib.pyplot.show()
        except TypeError as e:
            print("The provided matrix contains non-numeric"
                  " values. Please make sure that all the matrix's values are numbers")
            logging.error(e)
        except: raise

    @staticmethod
    def visualize_dictionary(dictionary: dict):
        """
        visualizes a dictionary as a column graph
        :param dictionary :type dict: the dictionary to be visualized
        :return :type None
        """
        """
               The resulted graph:

               values
               3                                   |
               2       |                           |
               1       |               |           |
                  (key, index)  (key, index)  (key, index)
               """
        try:
            dataframe = pandas.DataFrame.from_records([dictionary], index=[""])
            # dataframe: pandas.DataFrame = pandas.DataFrame(data=dictionary, index=[0])
            df_lists = dataframe[list(dictionary.keys())].unstack().apply(pandas.Series)
            df_lists.plot.bar(rot=0, cmap=matplotlib.pyplot.cm.jet, fontsize=8, width=0.7, figsize=(8, 4))
            matplotlib.pyplot.show()
        except TypeError as e:
            print("The provided dictionary contains non-numeric"
                  " values. Please make sure that all the dictionary's values are numbers")
            logging.error(e)
        except: raise
