import time
import json
import random
import logging
import urllib.parse

from scrapy import signals
from scrapy.http import Response
from scrapy.http.cookies import CookieJar
from scrapy.exceptions import CloseSpider
import requests.utils

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

import mysql.connector.pooling

logger = logging.getLogger(__name__)

class RandomProxyUACookiesMiddleware(object):
    """docstring for RandomProxyUACookiesMiddleware"""
    def __init__(self, crawler, cnxpool, max_cookies_counter):
        self.crawler = crawler
        self.cnxpool = cnxpool
        self.max_cookies_counter = max_cookies_counter

        # self.timeout = 10
        # self.browser = webdriver.Chrome()
        # self.browser.set_window_size(1400, 700)
        # self.browser.set_page_load_timeout(self.timeout)
        # self.wait = WebDriverWait(self.browser, self.timeout)
        # self.chrome_options = Options()
        # self.chrome_options.add_argument('--headless')

    def __del__(self):
        del self.cnxpool

    def get_random_proxy(self):
        """
        从数据库中获取proxy
        :return: proxy_id, proxy
        """
        total_proxy = 1
        offset = random.randint(0, total_proxy - 1)
        status = '0'

        sql = 'SELECT id, proxy FROM proxies WHERE status = \'%s\' AND web = \'gaode_map\' LIMIT %s,1' % (status, offset)
        cnx = self.cnxpool.get_connection()
        cursor = cnx.cursor()
        cursor.execute(sql)
        proxy = cursor.fetchone()

        cursor.close()
        cnx.close()

        logger.debug(sql)
        if proxy:
            logger.debug('Random proxy id: %s\t proxy: %s' % proxy)
        else:
            logger.debug('Random proxy: %s' % proxy)
        return proxy

    def get_random_ua(self):
        """
        从数据库中获取ua
        :params ua_id: ua_id
        :return: ua
        """
        total_ua = 250
        offset = random.randint(0, total_ua - 1)

        sql = 'SELECT id, ua FROM ua LIMIT %s,1' % offset
        cnx = self.cnxpool.get_connection()
        cursor = cnx.cursor()
        cursor.execute(sql)
        ua = cursor.fetchone()

        cursor.close()
        cnx.close()

        logger.debug('Random ua id: %s\t ua:%s' % ua)
        return ua

    def get_cookies(self, proxy_id, ua_id):
        """
        从数据库中获取proxy-ua所对应的cookies
        :params proxy_id: proxy_id
        :params ua_id: ua_id
        :return: (cookies_id, cookies, cookies_counter)
        """

        sql = 'SELECT id, cookies, counter FROM cookies WHERE proxy_id = %s AND ua_id = %s LIMIT 1'

        cnx = self.cnxpool.get_connection()
        cursor = cnx.cursor()
        cursor.execute(sql, (proxy_id, ua_id))
        cookies = cursor.fetchone()

        cursor.close()
        cnx.close()

        logger.debug('Random cookies id: %s\tcookies: %s\tcounter: %s' % cookies)
        return cookies

    def update_cookies(self, cookies_id, cookies, cookies_counter, info, status=None):
        """
        更新数据库中的cookies信息
        :params cookies_id: cookies_id
        :params cookies: cookies
        :params cookies_counter: proxy-ua的使用次数
        :params info: 相关信息（url中的query信息）
        :params status: cookies状态（0: 正常，otherwise: 异常）
        """
        cnx = self.cnxpool.get_connection()
        # cookies = None

        # 将Cookies转换成json
        if cookies:
            cookies_json = json.dumps(cookies)
        else:
            cookies_json = ''
        sql = ("""UPDATE `cookies` SET `cookies` = %s, `counter` = %s, `status` = %s """
                """, `info` = %s, `ts` = %s WHERE `id` = %s""")
        cursor = cnx.cursor()
        cursor.execute(sql, (cookies_json, cookies_counter, status, info, int(time.time()), cookies_id))
        cnx.commit()

        cursor.close()
        cnx.close()

    def block_proxy(self, proxy_id, status):
        """
        冻结代理
        :params proxy_id: proxy_id
        :params status: 状态信息
        """
        sql = ('UPDATE proxies SET status = %s, ts = %s, counter = '
                '(SELECT SUM(c.`counter`) FROM `cookies` as c WHERE c.`proxy_id` = %s) WHERE id = %s')

        cnx = self.cnxpool.get_connection()
        cursor = cnx.cursor()
        cursor.execute(sql, ('异常-:%s' % status, int(time.time()), proxy_id, proxy_id))
        cnx.commit()

        cursor.close()
        cnx.close()

    def process_request(self, request, spider):
        random_proxy = self.get_random_proxy()

        if random_proxy:
            proxy_id, proxy = random_proxy            
        else:
            # self.crawler.engine.close_spider(spider, 'closespider_proxyusedup')  
            raise CloseSpider('proxy used up')          

        random_ua = self.get_random_ua()
        if random_ua:
            ua_id, ua = random_ua

        cookies_id, cookies, counter = self.get_cookies(proxy_id, ua_id)
        proxies = {'https': 'https://%s' % proxy, 'http': 'http://%s' % proxy}

        # cookies使用次数过多时，更新cookies
        if counter % self.max_cookies_counter == 0:
            cookies = None
            request.cookies = {}

        # 如果cookies不为空，则将cookies添加至request中
        if cookies:
            # 将string 类型的cookies转换成cookiejar
            request.cookies = json.loads(cookies)
        else:
            logger.debug('Chrome is starting')
            chrome_options = Options()
            # chrome_options.add_argument("--headless")
            # 设置ua
            chrome_options.add_argument('--user-agent=%s' % ua)

            browser = webdriver.Chrome(chrome_options=chrome_options)
            try:
                browser.get('https://www.amap.com')
                input = browser.find_element_by_id('searchipt')
                input.send_keys('美食')
                input.send_keys(Keys.ENTER)
                wait = WebDriverWait(browser, 10)
                wait.until(EC.presence_of_element_located((By.ID, 'maptoolbox')))                
            except Exception as e:
                logger.warn('Error in selenium')
            else:
                cookies = browser.get_cookies()
                request.cookies = cookies
            finally:
                time.sleep(5)
                browser.close()
                
        if cookies:
            jar = CookieJar()
            cookies = self._get_request_cookies(jar, request)
            for cookie in cookies:
                jar.set_cookie_if_ok(cookie, request)

            # set Cookie header
            request.headers.pop('Cookie', None)
            jar.add_cookie_header(request)

        # 设置ua
        request.headers.setdefault(b'User-Agent', ua)

        request.meta['proxies'] = proxies
        request.meta['proxy_id'] = proxy_id
        request.meta['ua'] = ua
        request.meta['cookies_counter'] = counter
        request.meta['cookies_id'] = cookies_id

        # return request

    def process_response(self, request, response, spider):
        proxy_id = request.meta['proxy_id']
        cookies_id = request.meta['cookies_id']        
        cookies_counter = request.meta['cookies_counter']

        # 获取Response中返回的cookies
        logger.debug('0 After response cookies: %s' % request.cookies)

        cookies = request.cookies

        cookies_counter += 1

        url = response.url
        logger.debug(response.url)
        query = urllib.parse.urlsplit(response.url).query
        

        try:
            response_dict = json.loads(response.text)

            # response_dict = {'data': 0}

            logger.debug('response: %s' % response_dict['status'])

            if response_dict['status'] == '110':
                # 当response_dict['status'] 等于 101时，说明高德已经检测到抓取行为，
                # 该代理可能已经被检测到，冻结该代理，并将当前请求重新加入任务队列

                # 记录发生异常的请求信息                
                self.update_cookies(cookies_id, cookies, cookies_counter, 
                    query, response.text)
                logger.debug('cookies_id: %s\t cookies:%s\t cookies_counter:%s' % 
                    (cookies_id, cookies, cookies_counter))

                # 冻结代理
                self.block_proxy(proxy_id, response.text)

                # 设置响应码为601，进行Retry
                response.status = 601
                return response

            elif response_dict['status'] != '1':
                # 记录发生异常的请求信息                
                self.update_cookies(cookies_id, cookies, cookies_counter, 
                    query, response.text)
                logger.debug('cookies_id: %s\t cookies:%s\t cookies_counter:%s' % 
                    (cookies_id, cookies, cookies_counter))
                # 设置响应码为602，进行Retry
                response.status = 602
                return response

            else:
                # 更新cookies信息
                self.update_cookies(cookies_id, cookies, cookies_counter, query, response_dict['status'])
                return response

        except Exception as e:
            logger.error('proxies: %s\nua: %s\ncookies: %s\nquery: %s\n' % 
                (request.meta['proxies'], request.meta['ua'], cookies, query), 
                exc_info=True)
            # 设置响应码为602，进行Retry
            response.status = 602
            return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings

        dbconfig=dict(
            host = settings['MYSQL_HOST'],#读取settings中的配置
            db = settings['MYSQL_DBNAME'],
            user = settings['MYSQL_USER'],
            passwd = settings['MYSQL_PASSWD'],
        )

        max_cookies_counter = settings['MAX_COOKIES_COUNTER']

        cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name = "pucpool",
                                                      pool_size = 3, **dbconfig)
        return cls(crawler, cnxpool, max_cookies_counter)

    def _format_cookie(self, cookie):
        # build cookie string
        cookie_str = '%s=%s' % (cookie['name'], cookie['value'])

        if cookie.get('path', None):
            cookie_str += '; Path=%s' % cookie['path']
        if cookie.get('domain', None):
            cookie_str += '; Domain=%s' % cookie['domain']

        return cookie_str

    def _get_request_cookies(self, jar, request):
        if isinstance(request.cookies, dict):
            cookie_list = [{'name': k, 'value': v} for k, v in \
                    six.iteritems(request.cookies)]
        else:
            cookie_list = request.cookies

        cookies = [self._format_cookie(x) for x in cookie_list]
        headers = {'Set-Cookie': cookies}
        response = Response(request.url, headers=headers)

        return jar.make_cookies(response, request)