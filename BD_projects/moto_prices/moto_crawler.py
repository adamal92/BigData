# imports
import logging
import os

from io import TextIOWrapper
import subprocess, sys, time


DIRS_TILL_ROOT: int = 3
DELIMETER = "\\"  # "/"


class MotoCrawler:
    @staticmethod
    def get_file(save_as: str="motorcycles.jl") -> TextIOWrapper:
        """

        :return:
        """
        scrapy_crawler_path: str = ""
        for directory in os.path.dirname(__file__).split(DELIMETER)[:-DIRS_TILL_ROOT]:
            scrapy_crawler_path += f"{directory}\\"
        cwd_path = scrapy_crawler_path
        scrapy_crawler_path += r"BigData\BD_projects\moto_prices\scrapy_spider.py"

        # O for overriding, o for appending to file
        os.system(f'scrapy runspider "{scrapy_crawler_path}" -O {save_as} -L ERROR')

        with open(save_as) as file: return file


# def main():
#     start = time.time()
#
#     # loggers
#     py4j_logger = logging.getLogger('py4j.java_gateway')  # py4j logs
#     py4j_logger.setLevel(logging.ERROR)
#
#     matplotlib_logger = logging.getLogger('matplotlib')  # matplotlib logs
#     matplotlib_logger.setLevel(logging.ERROR)
#
#     # logging.basicConfig(level=logging.WARNING)
#
#     # scrapy
#     file: TextIOWrapper = get_file()
#     file_path: str = f"{os.getcwd()}\\{file.name}"
#     # file_path: str = f"{os.getcwd()}\\quotes.jl"
#     logging.debug(file_path)
#
#     print("OK Total Time: %s seconds" % (time.time() - start))

