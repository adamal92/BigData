# imports
import random, csv, logging
from Hadoop.hdfs import HDFS_handler
import os


class RandomFile:
    """

    """
    # static
    matrix: list = []
    CSV_PATH: str = r"C:\Users\adam l\Desktop\random.csv"

    @staticmethod
    def write(range_tup: tuple=(0, 1001, 1), csv_path: str=CSV_PATH, randint_tup: tuple=(-100, 100)):
        """
        write random numbers to csv
        :param range_tup: how many indexes
        :param csv_path:
        :param randint_tup: range of random values
        :return:
        """
        with open(csv_path, "w+", newline='') as csv_file:
            spamwriter = csv.writer(csv_file, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow(["index", "random integer"])
            for index in range(range_tup[0], range_tup[1], range_tup[2]):
                spamwriter.writerow([index, random.randint(randint_tup[0], randint_tup[1])])

    @staticmethod
    def read(csv_path: str=CSV_PATH) -> list:
        """
        read from csv into list
        :param csv_path:
        :return:
        """
        with open(csv_path, newline='') as csv_file2:
            spamreader: csv.reader = csv.reader(csv_file2, delimiter=' ', quotechar='|')
            for row in spamreader:
                print(', '.join(row))
                row_tup = list()
                for number in row:
                    try:
                        row_tup.append(int(number))
                    except ValueError as e:
                        print("provided value is not an integer")
                        logging.error(e)
                RandomFile.matrix.append(row_tup)
            RandomFile.matrix.pop(0)  # delete an empty list
        return RandomFile.matrix


RandomFile.write(range_tup=(0, 10**5+1, 1))
RandomFile.read()
print(RandomFile.matrix)

HDFS_handler.start()
os.system("hdfs dfs -help get")
os.system(f"hdfs dfs -put -f \"{RandomFile.CSV_PATH}\" /user/hduser")
os.system(HDFS_handler.LIST_FILES)
os.system("hdfs dfs -cat /user/hduser/random.csv")
os.system(HDFS_handler.HELP)





    # print(type(spamreader))

    # matrix.append(
    #     (index, random.randint(-100, 100))
    # )

# with open(r"C:\Users\adam l\Desktop\random.csv", "w+") as csv:
#     csv.write(str(matrix))

        # spamwriter.writerow(['Spam'] * 5 + ['Baked Beans'])
        # spamwriter.writerow(['Spam', 'Lovely Spam', 'Wonderful Spam'])
        #
        # csv_file.write(str(
        #     (index, random.randint(-100, 100))
        # ))
