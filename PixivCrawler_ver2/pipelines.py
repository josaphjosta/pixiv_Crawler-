import os
import time
import random
import re
import requests
import OpenSSL
import pandas
from pandas import Series, DataFrame

from spiders.pixivspider import KEY_LIST

requests.adapters.DEFAULT_RETRIES = 5

dir_path = r'D:\pixiv_img'

class PixivcrawlerVer2Pipeline(object):
    def __init__(self):
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
            }
        # 'sec - ch - ua': '"Chromium";v = "92", " Not A;Brand";v = "99", "Google Chrome";v = "92"',
        # 'sec - ch - ua - mobile': '?0',
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
        for key in KEY_LIST:
            key_path = os.path.join(dir_path, key)
            if not os.path.exists(key_path):
                os.mkdir(key_path)
            if not os.path.exists(key_path):
                os.mkdir(key_path)

        self.img_list = DataFrame({'0': []})  # img & url
        self.src_list = DataFrame({'0': []})
        self.img_path_list = DataFrame({'img':[], 'path':[], 'artist':[], 'like':[], 'date':[]})
        self.lable_img_list = DataFrame({'lable':[], 'img':[]})
        self.key_lable_list = DataFrame({'key':[], 'lable':[]})
        self.url_len = 0
        self.img_count = 0


    def close_spider(self, spider):
        print('closing...' + str(spider.src_list))
        # self.save_history(spider)
        print("Pipeline OFF")

    def open_spider(self, spider):
        print("Pipeline ON")

        if os.path.exists('./img_path_list.csv'):
            self.img_path_list = pandas.read_csv('./img_path_list.csv')
            self.img_list = self.img_path_list['img']
            print('\t\thistory_img\n', self.img_path_list.info())
        else:
            self.img_list = Series([])
            self.img_path_list = DataFrame({'img':[], 'path':[], 'artist':[], 'like':[], 'date':[]})

        img_list_prev = Series()
        for root, dirs, files in os.walk(r'D:\pixiv_img'):
            for name in files:
                if name.endswith('.png') or name.endswith('.jpg'):
                    img_list_prev = img_list_prev._append(Series([name]))
        self.img_list = self.img_list._append(img_list_prev)
        self.img_list.drop_duplicates(inplace=True)

        if os.path.exists('./lable_img_list.csv'):
            self.lable_img_list = pandas.read_csv('./lable_img_list.csv')
        else:
            self.lable_img_list = DataFrame({'lable':[], 'img':[]})

        if os.path.exists('./key_lable_list.csv'):
            self.key_lable_list = pandas.read_csv('./key_lable_list.csv')
        else:
            self.key_lable_list = DataFrame({'key':[], 'lable':[]})

        if os.path.exists('./src_list.csv'):
            history_src = pandas.read_csv('./src_list.csv')
            self.src_list = history_src['src']
            print('\t\thistory_src\n', history_src.info())
            spider.src_list = self.src_list
        else:
            self.src_list = Series([])

        print('\t\tcsv file loaded')

        url_len = len(self.src_list)

    def process_item(self, item, spider):

        print("\t----------PIPELINE----------")
        print("\tStart download")
        self.img_count += 1
        srcs = item['srcs']
        self.headers['Referer'] = item['referer']
        index = item['index']
        limited = item['limited']

        selections = item['selections']

        key_path = os.path.join(dir_path, item['category'])

        if not os.path.exists(key_path):
            os.mkdir(key_path)

        if not limited:
            key_path = os.path.join(key_path)
        for src_t in srcs:
            src_m = re.findall(r'(.*_p)0(\..*$)', src_t)[0]
            for i in range(index):
                src = src_m[0] + str(i) + src_m[1]
                fn = os.path.basename(src)
                filepath = os.path.join(key_path, fn)
                session = requests.session()
                session.keep_alive = False
                for cookie in spider.pre_cookies:
                    session.cookies.set(cookie['name'], cookie['value'])
                session.headers.clear()
                if not os.path.exists(filepath):
                    print(src)
                    if not self.img_list.empty:
                        if self.img_list.str.count(fn).sum():
                            print('\tImg has been downloaded before')
                            continue
                    try:
                        self.get_pic(spider, session, src, filepath, fn, selections, item['info'])
                        self.save_history(spider)
                    except:
                        print("\tDownload Error\nWaiting")
                        time.sleep(45)
                        self.get_pic(spider, session, src, filepath, fn, selections, item['info'])
                        self.save_history(spider)
                else:
                    if not (self.img_path_list['img'] == fn).any():
                        self.login_data(filepath, fn, selections, item['info'])
                        print('\tLost data updated')
                    print("\tImg existed or downloading failed")
        #     if (self.img_count + 1) % 100 == 0:
        #         self.save_history(spider)
        print("\t----------PIPELINE_T----------")
        return item

    def get_pic(self, spider, session, src, filepath, fn, selections, info, verify=True):
        r = session.get(src, headers=self.headers, timeout=(300, 300), verify=verify)
        with open(filepath, 'wb') as fp:
            fp.write(r.content)
        print("\t\tDownload completed")
        self.login_data(filepath, fn, selections, info)

        time.sleep(random.randint(1, 3))

    def login_data(self, filepath, fn, selections, info):
        self.img_list = self.img_list._append(Series([fn]), ignore_index=True)
        self.img_list.drop_duplicates(inplace=True)
        self.img_path_list = self.img_path_list._append(DataFrame({'img':[fn], 'path':[filepath], 'artist':[info['artist']], 'like':[info['like']], 'date':[info['str_mon']]}), ignore_index=True)
        self.img_path_list.drop_duplicates(inplace=True)

        for key in selections:
            if (not len(self.key_lable_list)) or (not (self.key_lable_list['key'] == key).any()):
                lable_size = len(self.key_lable_list)
                self.key_lable_list = self.key_lable_list._append(DataFrame({'key':[key], 'lable':[str(lable_size+1)]}), ignore_index=True)
            lable = self.key_lable_list.loc[self.key_lable_list['key'] == key]['lable'].to_list()[0]
            self.lable_img_list = self.lable_img_list._append(DataFrame({'lable':[lable], 'img':[fn]}), ignore_index=True)
            self.lable_img_list.drop_duplicates(inplace=True)

    def save_history(self, spider):
        self.src_list_save=DataFrame(data={'src': spider.src_list}).drop_duplicates()
        self.src_list_save.to_csv(path_or_buf='./src_list.csv', index=False)
        self.img_path_list.to_csv(path_or_buf='./img_path_list.csv', index=False)
        self.lable_img_list.to_csv(path_or_buf='./lable_img_list.csv', index=False)
        self.key_lable_list.to_csv(path_or_buf='./key_lable_list.csv', index=False)

        print('\t\tlength of url list :\t' + str(len(spider.src_list)))
        print('\t\tcrawled url :\t' + str(len(spider.src_list) - self.url_len))
        print('\t\thistory list saved')