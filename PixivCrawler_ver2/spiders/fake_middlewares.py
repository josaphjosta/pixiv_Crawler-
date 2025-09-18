import time
import scrapy
from scrapy import signals
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

COOKIES = []


def process_request(pre_driver, request, spider):
    scroll_down = request.url.endswith('_still')
    url = request.url.replace('_still', '')
    if url == "https://accounts.pixiv.net/login":
        if COOKIES:
            pre_driver.delete_all_cookies()
            for cookie in COOKIES:
                pre_driver.add_cookie(cookie)
        else:
            response = get_cookies(pre_driver, request, spider)
            tab_discard(pre_driver)
            response.pri_url = url
            response.rf = False
            response.pre_driver = pre_driver
            return response
    else:
        try:
            # pre_driver.set_page_load_timeout(10)
            # pre_driver.set_script_timeout(10)
            pre_driver.get(url)
        except:
            try:
                pre_driver.execute_script('window.stop()')
                pre_driver.switch_to.window(pre_driver.window_handles[0])
                try:
                    pre_driver.execute_script('window.open("{}")'.format(url))
                except:
                    pass
                try:
                    pre_driver.execute_script('window.stop()')
                except:
                    pass
                if len(pre_driver.window_handles) > 1:
                    pre_driver.close()
                pre_driver.switch_to.window(pre_driver.window_handles[0])
                tab_discard(pre_driver)
                pre_driver.get(url)
            except:
                # pre_driver.execute_script('window.stop()')
                pass
            print("\tWindow switched")

        try:
            # pre_driver.switch_to.window(pre_driver.window_handles[0])
            wait_until(pre_driver, secends=5, xpaths='//div[@role="presentation"]//a')
            wait_until(pre_driver, secends=5, xpaths='//div//section//figcaption//dd[@title]')
            if not scroll_down:
                print('\t dwon')
                js = 'document.documentElement.scrollTop=10000'
                for i in range(5):
                    pre_driver.execute_script(js)
                    wait_until(pre_driver, secends=5, xpaths='//aside//ul/li//a')
                    pre_driver.execute_script(js)
                    wait_until(pre_driver, secends=5, xpaths='//aside//ul/li//a')
                    time.sleep(0.5)
            else:
                time.sleep(0.5)
        except:
            print("\tTime out")
            pass
        try:
            # pre_driver.switch_to.window(pre_driver.window_handles[0])
            try:
                pre_driver.execute_script('window.stop()')
            except:
                pass
            response = scrapy.http.HtmlResponse(url=pre_driver.current_url,
                                                body=pre_driver.page_source,
                                                request=request,
                                                encoding='utf-8')
        except:
            try:
                pre_driver.switch_to.window(pre_driver.window_handles[0])
                try:
                    pre_driver.execute_script('window.open("{}")'.format(url))
                except:
                    pre_driver.execute_script('window.stop()')
                if len(pre_driver.window_handles) > 1:
                    pre_driver.close()
                pre_driver.switch_to.window(pre_driver.window_handles[0])
                tab_discard(pre_driver)
                pre_driver.get(url)
            except:
                # pre_driver.execute_script('window.stop()')
                pass
            print("\tWindow switched")

            response = scrapy.http.HtmlResponse(url=pre_driver.current_url,
                                                body=pre_driver.page_source,
                                                request=request,
                                                encoding='utf-8')
        response.pre_driver = pre_driver
        response.pri_url = url
        return response


def get_cookies(pre_driver, re, spider):
    pre_driver.get(re.url)
    # login
    while True:
        con = input("login completed?(Y/n)")
        if con == 'y' or con == 'Y':
            pre_driver.switch_to.window(pre_driver.window_handles[0])
            response = scrapy.http.HtmlResponse(url=pre_driver.current_url,
                                                body=pre_driver.page_source,
                                                request=re,
                                                encoding='utf-8')
            spider.pre_cookies = pre_driver.get_cookies()
            COOKIES = spider.pre_cookies
            return response

def return_element(pre_driver, ele):
    return pre_driver.execute_script("return arguments[0].shadowRoot", ele)

def tab_discard(pre_driver):
    try:
        # pre_driver.set_page_load_timeout(150)
        # pre_driver.set_script_timeout(150)
        url = "chrome://discards/"
        pre_driver.get(url=url)
        time.sleep(1)
        first_sd = return_element(pre_driver, pre_driver.find_element(By.CSS_SELECTOR, 'discards-main'))
        # second_sd = return_element(first_sd.find_element("iron-pages"))
        thr_sd = return_element(pre_driver, first_sd.find_element(By.CSS_SELECTOR, "discards-tab"))
        submit_tag = thr_sd.find_element(By.CLASS_NAME, 'is-auto-discardable-link')
        if thr_sd.find_element(By.CLASS_NAME, 'boolean-cell').text[0] == '✔':
            submit_tag.click()
        print("Tab discard completed")
    except:
        pass

def wait_until(pre_driver, secends, xpaths):
    try:
        WebDriverWait(pre_driver, secends).until(
            EC.presence_of_all_elements_located((By.XPATH, xpaths)))
    except:
        pass


