from io import TextIOWrapper

from textblob import TextBlob
import os

import nltk
from data_types_and_structures import DataTypesHandler
import os
from sentiment_text import recognize


nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('brown')
from nltk.stem import LancasterStemmer
"""
This file contains the server of the program.
"""


def recognize(file_string: TextIOWrapper, keyword: str) -> dict:
    """
        param file_string: A file that represent a data file
        param keyword: A string with a keyword to search
    """
    # Here goes the scraping result for articles .....
    feed = file_string.read()
    blob = TextBlob(feed)
    # print(blob.sentiment)
    # print(blob.tags)
    # print(blob.noun_phrases)
    # print(blob.sentiment.polarity)
    # print(blob.words)
    # print(blob.sentences)
    lst = LancasterStemmer()
    ret_dict = {}
    counter = 0
    ret_dict["connotation_list"] = []
    for sentence in blob.sentences:
        # print(sentence)
        for word in sentence.words:
            # print(lst.stem(word))
            if lst.stem(word) == lst.stem(keyword):
                counter += 1
                ret_dict["connotation_list"].append(
                    {
                        "polarity": sentence.sentiment.polarity,
                        "subjectivity": sentence.sentiment.subjectivity,
                        "sentence": sentence.correct()
                    }
                )
                # print(sentence.sentiment)
    ret_dict["times_found"] = counter
    ret_dict["total_connotation"] = {
        "polarity": blob.sentiment.polarity,
        "subjectivity": blob.sentiment.subjectivity
    }
    return ret_dict


def main():
    """
        Main function of the server.
        Includes the calls for conotation recognition functions and calls the data base queries
    """

    with open(fr'{os.getcwd()}\wiki.txt', "r", encoding='utf-8') as f:
        # recognize(f, "israel")
        DataTypesHandler.print_data_recursively(
            data=recognize(f, "israel"), print_dict=DataTypesHandler.PRINT_DICT
        )
    # feed = "the food at Ruby's place is awful"
    # blob = TextBlob(feed)
    # print(blob.sentiment)


if __name__ == "__main__":
    main()
