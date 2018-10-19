import time
import json
import random
import logging
import urllib.parse

from scrapy import signals
from scrapy.http.cookies import CookieJar
from scrapy.exceptions import CloseSpider
import requests.utils

import mysql.connector.pooling

logger = logging.getLogger(__name__)

class RandomProxyUACookiesMiddleware(object):
    """设置cookies ua proxy"""
    def __init__(self, crawler, cnxpool, max_cookies_counter, proxy_url, proxies, web):
        self.crawler = crawler
        self.cnxpool = cnxpool
        self.max_cookies_counter = max_cookies_counter
        self.proxy_url = proxy_url
        self.proxies = proxies
        self.web = web

    def __del__(self):
        del self.cnxpool

    def get_random_proxy_from_web(self, schema='https'):
        """
        从代理池中获取ua
        :params schema: https or http
        :params proxy_url: 代理池地址
        :return: id, proxy
        """
        proxy = None
        url = self.proxy_url
        try:
            proxy = 'https://113.128.148.119:8118'
            # response = requests.get(url=url)
            # if response and response.status_code == 200:
            #     re = json.loads(response.text)
            #     if 'error' in re and re['error'] == 0:
            #         proxy = '%s://%s:%s' % (schema, re['data']['host'], re['data']['port'])

        except Exception as e:
            proxy = None

        return proxy

    def get_random_proxy(self):
        """
        从数据库中获取proxy
        :return: proxy_id, proxy
        """
        total_proxy = 1000
        offset = random.randint(0, total_proxy - 1)
        status = '0'

        sql = 'SELECT id, proxy FROM proxies WHERE status = \'%s\' AND web = \'%s\' LIMIT %s,1' % (status, self.web, offset)
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

    def get_cookies_web(self, ua_id):
        """
        从数据库中获取ua所对应的cookies
        :params ua_id: ua_id
        :return: (cookies_id, cookies, cookies_counter)
        """

        sql = 'SELECT id, cookies, counter FROM cookies WHERE ua_id = %s AND web = %s LIMIT 1'

        cnx = self.cnxpool.get_connection()
        cursor = cnx.cursor()
        cursor.execute(sql, (ua_id, self.web))
        cookies = cursor.fetchone()

        cursor.close()
        cnx.close()

        logger.info('Random cookies id: %s\tcookies: %s\tcounter: %s' % cookies)
        return cookies

    def process_request(self, request, spider):
        schema = 'https'

        if self.proxy_url:      # 使用proxy pool
            proxy = self.get_random_proxy_from_web(schema)

            while not proxy:
                time.sleep(5)
                proxy = self.get_random_proxy_from_web(schema)
                
                if proxy and proxy in self.proxies:     # proxy正在被使用，重新获取proxy
                    proxy = None
                else:                                   # 将proxy加入集合，防止其他爬虫同时使用该proxy
                    self.proxies.append(proxy)
        else:
            pass

        ua_id, ua = self.get_random_ua()
        cookies_id, cookies, counter = self.get_cookies_web(ua_id)

        # 设置cookies
        if cookies:
            request.cookies = self._transform_cookies(cookies)
        elif self.web == 'baidu_map':
            request.cookies = {'MCITY': '-%s%%3A' % random.randint(100, 360)}

        # cookies使用次数过多时，重新获取cookies
        if counter // self.max_cookies_counter > 0:
            cookies = None
            if self.web == 'baidu_map':
                request.cookies = {'MCITY': '-%s%%3A' % random.randint(100, 360)}

        # 设置ua
        request.headers['User-Agent'] = ua

        # 防止redirect、retry时，重复添加proxy
        if 'proxy' in request.meta and request.meta['proxy'] in self.proxies:
            self.proxies.remove(request.meta['proxy'])        

        proxy = 'https://125.70.13.77:8080'
        # 设置proxy
        # request.meta['proxy'] = proxy
        self.proxies.append(proxy)

        request.meta['cookies_counter'] = counter
        request.meta['cookies_id'] = cookies_id

    def process_response(self, request, response, spider):
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain

        # 从集合中移除proxy
        if 'proxy' in request.meta and request.meta['proxy'] in self.proxies:
            self.proxies.remove(request.meta['proxy'])        
        pass
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings

        dbconfig=dict(
            host = settings['MYSQL_HOST'],#读取settings中的配置
            db = settings['MYSQL_DBNAME'],
            user = settings['MYSQL_USER'],
            passwd = settings['MYSQL_PASSWD'],
            # charset = 'utf8',#编码要加上，否则可能出现中文乱码问题
            # cursorclass = MySQLdb.cursors.DictCursor,
            # use_unicode = False,
        )

        max_cookies_counter = settings['MAX_COOKIES_COUNTER']

        cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name = "pucpool",
                                                      pool_size = 32, **dbconfig)

        proxy_url = settings['PROXY_URL']

        proxies = settings['PROXIES']
        web = settings['WEB']
        return cls(crawler, cnxpool, max_cookies_counter, proxy_url, proxies, web)

    def _transform_cookies(self, cookies_str):
        """
        将request.headers中的cookies转换成json
        :params cookies_str: str
        :return json
        """
        cookies = cookies_str.split(';')
        cookies_json = {}
        if cookies:            
            for c in cookies:
                c = c.strip()
                name, val = c.split('=', 1)
                cookies_json[name] = val
        return cookies_json


