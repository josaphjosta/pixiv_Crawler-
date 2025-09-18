# -*- coding: utf-8 -*-
import time
import random
import math
import scrapy
import lxml
from lxml.html import etree
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import re
from pandas import Series, DataFrame
from PixivCrawler_ver2.items import PixivcrawlerVer2Item

import fake_middlewares as fm

LOG_FILENAME = 'log.txt'


def log_info(info, end='\n'):
    with open(LOG_FILENAME, 'a', encoding='utf-8') as fp:
        print(info, end=end, file=fp)


LIKE = 2000
BREAD_CRAWL = True
SELECT = ''


KEY_DICT = {"name": ["tag", [LIKE, 0]]
}

KEY_LIST = [key for key, value in KEY_DICT.items()]
ART = ''

select_list = [
]


class PixivspiderSpider(scrapy.Spider):
    name = 'pixivspider'
    start_urls = ['https://accounts.pixiv.net/login']

    def __init__(self):
        super().__init__()
        init_like = 100000
        self.src_list = Series([])
        self.select_dict = DataFrame({'like': [], 'src': [], 'tag': [], 'limited': []})
        self.select_list = Series([])
        for t in select_list:
            init_like += 1
            src = t.replace('https://www.pixiv.net', '')
            self.select_dict = self.select_dict._append(
                DataFrame({'like': [init_like], 'src': [src], 'tag': ['pre_{}'.format(init_like)], 'limited': [1]}),
                ignore_index=True)
        self.tag_list = self.select_dict['tag'].drop_duplicates().to_list()
        self.check_u = 0

    def parse(self, response):
        print('\n--------H_MAIN_SPIDER----------')
        log_info('\n--------H_MAIN_SPIDER----------')
        origon, artist_page = self.get_artwork(response)

        if artist_page:
            print(len(origon), origon)
            i = 1
            while True:
                i = i + 1
                artist_url = re.sub(r'artworks.*', 'artworks?p=' + str(i), artist_page[0])
                response.pre_driver.get("https://www.pixiv.net" + artist_url)
                self.wait_until(response.pre_driver, secends=5, xpaths='//div[@type="illust"]//a/@href')
                time.sleep(1)
                artist_source = response.pre_driver.page_source
                tree = etree.HTML(artist_source)
                artworks_illust = tree.xpath('//div[@type="illust"]//a/@href')
                artwork_url_len = len(artworks_illust)
                print(artist_url, len(artworks_illust), artworks_illust)
                origon.extend(artworks_illust)
                if artwork_url_len < 48:
                    print(len(origon))
                    log_info(len(origon))
                    break
        else:
            print('\n---DOWNLOAD THE FIRST(MAIN)---')
            log_info('\n---DOWNLOAD THE FIRST(MAIN)---')
            for j_p in self.jojo_parse(response):
                yield j_p
            print('---DEALING WITH ITERS(MAIN)---\n')
            log_info('---DEALING WITH ITERS(MAIN)---\n')

        for i, ori_url in enumerate(origon):
            mk_url = "https://www.pixiv.net" + ori_url
            self.src_list = self.src_list._append(Series([ori_url]), ignore_index=True)
            print('\tmain processing {}/{}'.format(i + 1, len(origon)))
            log_info('\tmain processing {}/{}'.format(i + 1, len(origon)))
            if BREAD_CRAWL:
                request = scrapy.Request(mk_url, callback=self.dio_parse, dont_filter=True, priority=2)
                for iner_j in self.dio_parse(fm.process_request(response.pre_driver, request, self)):
                    yield iner_j
            else:
                mk_url = mk_url + '_still'
                request = scrapy.Request(mk_url, callback=self.dio_parse, dont_filter=True, priority=1)
                for j_p in self.jojo_parse(fm.process_request(response.pre_driver, request, self)):
                    yield j_p
        else:
            print('\n--------MAIN_SPIDER----------')
            log_info('\n--------MAIN_SPIDER----------')
            self.renew_tag_list()
            ori_url_list = self.select_uni_url()
            self.select_list = self.select_list._append(Series([ori_url_list[1]]), ignore_index=True)
            mk_url = "https://www.pixiv.net" + ori_url_list[1]
            print('\tlength of url list in this turn:\t' + str(len(origon)))
            log_info('\tlength of url list in this turn:\t' + str(len(origon)))
            print('\tSelected ori_url:{} LIKE: {} TAG: {} in {}'.format(mk_url, ori_url_list[0], ori_url_list[2],
                                                                        len(self.select_dict)))
            log_info('\tSelected ori_url:{} LIKE: {} TAG: {} in {}'.format(mk_url, ori_url_list[0], ori_url_list[2],
                                                                           len(self.select_dict)))
            print(self.select_dict.head(15))
            log_info(self.select_dict.head(15))

            yield scrapy.Request(mk_url, callback=self.parse, dont_filter=True, priority=1)

        print('--------T_MAIN_SPIDER----------\n')
        log_info('--------T_MAIN_SPIDER----------\n')

    def dio_parse(self, response):
        print('\n--------H_DIO_SPIDER----------')
        log_info('\n--------H_DIO_SPIDER----------')
        print('\n---DOWNLOAD THE FIRST(DIO)---')
        log_info('\n---DOWNLOAD THE FIRST(DIO)---')
        selections = response.xpath('//footer//li//text()').getall()
        str_mon = response.xpath('//figcaption//div[@title]//text()').get()
        like = response.xpath('//div//section//figcaption//dd[@title]//text()').getall()
        if len(like):
            like = like[1]
        else:
            like = '9999'
        like = re.sub(r'[^\d]', '', like)
        w_like = self.weight_like(int(like), str_mon)
        key = self.match_key(selections)

        if key and self.is_selected(w_like, selections[0], key):
            origon = self.get_artwork(response, dio=True)[0]
        else:
            origon = []
        for j_p in self.jojo_parse(response):
            yield j_p
        # yield next(self.jojo_parse(response))
        print('\n---DEALING WITH ITERS(DIO)---\n')
        log_info('\n---DEALING WITH ITERS(DIO)---\n')
        for i, ori_url in enumerate(origon):
            if not self.src_list.empty:
                if self.is_crawled(ori_url):
                    print('\t\tdio processed already {}/{}\n'.format(i + 1, len(origon)))
                    log_info('\t\tdio processed already {}/{}\n'.format(i + 1, len(origon)))
                    continue
            print('\t\tdio processing {}/{}'.format(i + 1, len(origon)))
            log_info('\t\tdio processing {}/{}'.format(i + 1, len(origon)))
            mk_url = "https://www.pixiv.net" + ori_url + '_still'
            self.src_list = self.src_list._append(Series([ori_url]), ignore_index=True)
            request = scrapy.Request(mk_url, callback=self.jojo_parse, dont_filter=True, priority=3)
            for j_p in self.jojo_parse(fm.process_request(response.pre_driver, request, self)):
                yield j_p
            # yield next(self.jojo_parse(fm.process_request(response.pre_driver, request, self)))
            # yield scrapy.Request(mk_url, callback=self.jojo_parse, dont_filter=True, priority=3)
        else:
            print('\tlength of url list in this turn(in iteration):\t' + str(len(origon)))
            log_info('\tlength of url list in this turn(in iteration):\t' + str(len(origon)))
            print('--------T_DIO_SPIDER----------\n')
            log_info('--------T_DIO_SPIDER----------\n')

    def jojo_parse(self, response):
        print('\t--------H_JOJO_SPIDER----------')
        log_info('\t--------H_JOJO_SPIDER----------')
        try:
            current_url = response.pri_url
            # try:
            selections = response.xpath('//footer//li//text()').getall()
            artist = response.xpath('//section//h2//a//text()').get()
            print('\t\t' + str(selections))
            log_info('\t\t' + str(selections))
            str_mon = response.xpath('//figcaption//div[@title]//text()').get()
            like = response.xpath('//div//section//figcaption//dd[@title]//text()').getall()
            if len(like):
                like = like[1]
            else:
                like = '9999'
            like = re.sub(r'[^\d]', '', like)
            w_like = self.weight_like(int(like), str_mon)
            limited = 0
            select_key = self.select_mode(w_like, selections, artist, response)
            if select_key:
                print(
                    "\t\tSELECTION({}): {} \t{} LIKE: {}/{} DATE:{} {}".format(SELECT, select_key, response.url, w_like,
                                                                               like, str_mon, 'ACCESS'))
                log_info(
                    "\t\tSELECTION({}): {} \t{} LIKE: {}/{} DATE:{} {}".format(SELECT, select_key, response.url, w_like,
                                                                               like, str_mon, 'ACCESS'))
                p_items = {'src': response.xpath('//div[@role="presentation"]/a/@href').getall(),
                           'referer': response.pri_url}
                tag = response.xpath('//div[@role="presentation"]//div[@aria-label]//text()').get()

                print('\t\timg tag: \t' + str(tag) + ' limited: ', limited)
                log_info('\t\timg tag: \t' + str(tag) + ' limited: ' + str(limited))
                print('\t\ttag list ' + str(self.tag_list) + 'CHECK_U ', self.check_u)
                if tag:
                    p_items['index'] = int(tag.replace('1/', ''))
                else:
                    p_items['index'] = 1
                print('\t\t' + str(p_items['src']))
                log_info('\t\t' + str(p_items['src']))
                info = {'artist': artist, 'str_mon': str_mon, 'like': like}
                n_item = PixivcrawlerVer2Item(srcs=p_items['src'], referer=p_items['referer'], index=p_items['index'],
                                              category=select_key, selections=selections, limited=limited, info=info)
                yield n_item
            else:
                print(
                    "\t\tSELECTION({}): {} \t{} LIKE: {}/{} DATE:{} {}".format(SELECT, selections[0], response.url,
                                                                               w_like,
                                                                               like, str_mon, 'PASS'))
                log_info("\t\tSELECTION({}): {} \t{} LIKE: {}/{} DATE:{} {}".format(SELECT, selections[0], response.url,
                                                                                    w_like,
                                                                                    like, str_mon, 'PASS'))
                yield PixivcrawlerVer2Item(srcs='', referer='', index='index', category='None', limited='None',
                                           selections=[])
            print('\t--------T_JOJO_SPIDER----------\n')
            log_info('\t--------T_JOJO_SPIDER----------\n')
        except:
            print('Get source error')
            yield PixivcrawlerVer2Item(srcs='', referer='', index='index', category='None', limited='None',
                                       selections=[])
        #     con = self.src_list.isin([current_url]) == False
        #     self.src_list = self.src_list[con]

    def get_artwork(self, response, dio=False):

        pattern = r'/artworks/\d+'
        artworks = response.xpath('//aside//ul/li//div[@type]//a/@href').getall()
        artworks_illust = response.xpath('//div[@type="illust"]//a/@href').getall()
        artworks.extend(artworks_illust)

        pa = r'/users/\d+?/artworks'
        artist_page = []
        artist_url = []
        if re.search('user', response.url):
            artist_url = response.xpath('//div/a/@href').getall()
            print('\tMODE ARTIST')
            log_info('\tMODE ARTIST')
        for a_u in artist_url:
            if re.search(pa, a_u):
                artist_page.append(a_u)
        if dio:
            try:
                origon = re.findall(pattern, str(artworks))
            except:
                origon = re.findall(pattern, str(artworks))
        else:
            origon = re.findall(pattern, str(artworks))

        if origon:
            print('\t\tcrawled url: \t' + str(len(origon)))
            log_info('\t\tcrawled url: \t' + str(len(origon)))
        # origon = origon[::-1]

        return origon, artist_page

    def match_key(self, selections):
        sel_string = str(selections)
        if not KEY_DICT:
            return 'All'
        if re.search('AI生成', sel_string, re.I):
            return False
        for key in KEY_LIST:
            for key_word in KEY_DICT[key][0]:
                if re.search(key_word, sel_string, re.I):
                    return key
        return False

    def loop_tag(self):
        self.tag_list.reverse()
        tag = self.tag_list.pop()
        self.tag_list.reverse()
        if not tag.startswith('pre_'):
            self.tag_list.append(tag)
        return tag

    def select_url(self):
        try:
            self.select_dict.drop_duplicates(inplace=True)
            tag = self.tag_list[0]
            print('\tSelecting TAG\t', tag)
            log_info('\tSelecting TAG\t' + str(tag))
            con = self.select_dict['tag'] == tag
            if con.any():
                tag_dict = self.select_dict[con]
                con_u = tag_dict['limited'] == 0
                tag_dict = tag_dict[con_u]
                if tag_dict.any().any():
                    tag_dict = tag_dict.sort_values('like')
                    se_index = tag_dict.index[-1]
                else:
                    tag_dict = self.select_dict[con]
                    tag_dict = tag_dict.sort_values('like')
                    se_index = tag_dict.index[-1]
            else:
                print('\tNo matching')
                log_info('\tNo matching')
                se_index = random.randint(0, len(self.select_dict) - 1)
            url_dict = self.select_dict.loc[se_index].to_list()
            self.select_dict = self.select_dict.drop(index=se_index)
            self.select_dict.sort_values('like', inplace=True)
            print("\tTAG list ", self.tag_list)
            log_info("\tTAG list " + str(self.tag_list))
            return url_dict
        except:
            print('\t\tselect error')
            log_info('\t\tselect error')
            se_index = random.randint(0, len(self.select_dict) - 1)
            url_dict = self.select_dict.loc[se_index].to_list()
            self.select_dict = self.select_dict.drop(index=se_index)
            self.select_dict.sort_values('like', inplace=True)
            return url_dict

    def renew_tag_list(self):
        new_tag_list = self.select_dict['tag'].drop_duplicates().to_list()
        for tag in new_tag_list:
            if tag not in self.tag_list:
                self.tag_list.append(tag)

    def select_uni_url(self):
        url_dict = self.select_url()
        if not self.select_list.empty:
            while self.select_list.str.count(url_dict[1]).sum():
                url_dict = self.select_url()
        self.loop_tag()
        self.check_u = (self.check_u + 1) % 2
        return url_dict

    def is_selected(self, like, selection, key):
        try:
            current_like = KEY_DICT[key][1]
            print("\t\tKEY:{}  CLIKE:{}".format(key, current_like))
            log_info("\t\tKEY:{}  CLIKE:{}".format(key, current_like))
        except:
            current_like = LIKE
        if int(like) > current_like:
                return True
        return False

    def select_mode(self, like, selections, artist, response):
        if selections:
            selection = selections[0]
        else:
            return False
        key = self.match_key(selections)
        limited = 0
        url = response.url.replace('https://www.pixiv.net', '')
        if self.is_selected(like, selection, key):
            if key:
                self.select_dict = self.select_dict._append(
                    DataFrame({'like': [int(like)], 'src': [url], 'tag': [key], 'limited': [limited]}),
                    ignore_index=True)
                print('\t\tNOTE ONLY ', end='')
                log_info('\t\tNOTE ONLY ', end='')
            return key
        elif int(like) >= 20000 or re.search(ART, artist, re.I):
            key = ' '
            self.select_dict = self.select_dict._append(
                DataFrame({'like': [int(like)], 'src': [url], 'tag': [key], 'limited': [limited]}),
                ignore_index=True)
            print('\t\tNOTE R ', end='')
            log_info('\t\tNOTE R ', end='')
            return key
        return False

    def weight_like(self, like, str_mon, k=0.02):
        # return like
        try:
            t = time.localtime()
            p = r'(\d+).+?(\d+).+?(\d+)'
            date = re.findall(p, str_mon)[0]
            if t.tm_year - int(date[0]) > 30:
                print('OUT DATED: ', int(date[0]))
                return 0
            delta_mon = (t.tm_year - int(date[0])) * 12 + t.tm_mon - int(date[1]) + (t.tm_mday - int(date[2])) / 30
            return like // (math.log(0.002 * k * delta_mon * like + 1) + 1)
        except:
            return like

    def is_crawled(self, ori_url):
        if not self.src_list.empty:
            if self.src_list.str.count(ori_url).sum():
                # print('\tUrl have been crawled before')
                return True
            else:
                return False

    def wait_until(self, pre_driver, secends, xpaths):
        try:
            WebDriverWait(pre_driver, secends).until(
                EC.presence_of_all_elements_located((By.XPATH, xpaths)))
            time.sleep(2)
        except:
            pass

    def get_time(self):
        return '\t{}.{}.{} {}m {}s'.format(time.localtime()[0], time.localtime()[1], time.localtime()[2],
                                           time.localtime()[3], time.localtime()[4], time.localtime()[5])
