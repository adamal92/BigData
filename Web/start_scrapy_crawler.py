import os


def main():
    path = r"C:\Users\adam l\Desktop\python files\BigData\Web\scrapy_web_crawler.py"

    os.system(f"scrapy runspider \"{path}\" -o quotes.jl")

    # os.system(
    # "scrapy runspider \"C:\\Users\\adam l\\Desktop\\python files\\BigData\\Web\\scrapy_web_crawler.py\" -o quotes.jl"
    # )


if __name__ == '__main__':
    main()
