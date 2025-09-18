from scrapy import cmdline
import os
import sys
ROOT_DIR = os.path.abspath(r'D:\Python_Project\PixivCrawler_ver2\PixivCrawler_ver2\spiders')
sys.path.append(ROOT_DIR)

cmdline.execute("scrapy crawl pixivspider ".split())

