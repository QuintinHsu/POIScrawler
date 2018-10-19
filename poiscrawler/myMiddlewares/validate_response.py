import time
import json
import logging
from scrapy import signals
from scrapy.http.cookies import CookieJar
from scrapy.exceptions import CloseSpider
import urllib.parse

import mysql.connector.pooling

logger = logging.getLogger(__name__)

class ValidateResponseMiddleware(object):
    """验证百度返回的结果"""
    def __init__(self, crawler, cnxpool, max_cookies_counter, proxies):
        self.crawler = crawler
        self.cnxpool = cnxpool
        self.max_cookies_counter = max_cookies_counter
        self.proxies = proxies

    def __del__(self):
        del self.cnxpool

    def process_request(self, request, spider):
        pass

    def process_response(self, request, response, spider):
        cookies_id = request.meta['cookies_id']        
        cookies_counter = request.meta['cookies_counter']

        # 防止同一代理之间使用频率过快
        time.sleep(5) 

        # 从集合中释放proxy
        if 'proxy' in request.meta and request.meta['proxy'] in self.proxies:
            self.proxies.remove(request.meta['proxy'])     

        cookies_counter += 1

        url = response.url
        query = urllib.parse.urlsplit(response.url).query

        # 获取cookies
        request_cookies = request.headers.get('Cookie')
        response_cookies = response.headers.getlist('Set-Cookie')
        cookies = self._merge_cookies(request_cookies, response_cookies)
        logger.info('After response cookies: %s' % cookies)

        try:
            data_security = -1
            response_dict = json.loads(response.text)

            if 'result' in response_dict:
                data_security = response_dict['result']['data_security_filt_res']

            if 'result' not in response_dict and data_security > 0:
                # 当data_security > 0时，说明百度对结果进行了筛选，不是完整的结果，
                # 该代理可能已经被检测到，冻结该代理，并将当前请求重新加入任务队列

                # 记录发生异常的请求信息
                err_info = ''
                if 'result' not in response_dict:
                    err_info = 'result not in response_dict'
                else:
                    err_info = data_security

                # 发生异常时，通过设置cookies的使用次数，使该cookies失效
                cookies_counter = self.max_cookies_counter + 100
                self.update_cookies(cookies_id, cookies, cookies_counter, 
                    query, err_info)

                logger.error(response.text)

                # 冻结代理
                # self.block_proxy(proxy_id, data_security)

                # 设置响应码为601，进行Retry
                response.status = 601
                return response

            else:
                # 更新cookies信息                
                self.update_cookies(cookies_id, cookies, cookies_counter, query, data_security)
                return response

        except Exception as e:
            logger.error('proxy: %s\nua: %s\ncookies: %s\nquery: %s\n' % 
                (request.meta['proxy'], request.headers['User-Agent'], cookies, query), 
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

        proxies = settings['PROXIES']

        return cls(crawler, cnxpool, max_cookies_counter, proxies)

    def update_cookies(self, cookies_id, cookies, cookies_counter, info, status=None):
        """
        更新数据库中的cookies信息
        :params cookies_id: cookies_id
        :params cookies: cookies string
        :params cookies_counter: proxy-ua的使用次数
        :params info: 相关信息（url中的query信息）
        :params status: cookies状态（0: 正常，otherwise: 异常）
        """
        cnx = self.cnxpool.get_connection()
        # cookies = None

        
        sql = ("""UPDATE `cookies` SET `cookies` = %s, `counter` = %s, `status` = %s """
                """, `info` = %s, `ts` = %s WHERE `id` = %s""")
        cursor = cnx.cursor()
        cursor.execute(sql, (cookies, cookies_counter, status, info, int(time.time()), cookies_id))
        cnx.commit()

        cursor.close()
        cnx.close()

    def insert_cookies(self, proxy, cookies, ua, info, status):
        """
        将新的cookies信息插入数据库
        :params proxy: cookies_id
        :params cookies: cookies string
        :params ua: ua
        :params info: 相关信息（url中的query信息）
        :params status: cookies状态（0: 正常，otherwise: 异常）
        """
        cnx = self.cnxpool.get_connection()
        sql = "INSERT INTO `cookies` SET `cookies` = %s, `proxy` = %s, `ua` = %s, `counter` = %s, `status` = %s, `info` = %s, `web` = \'baidu_map\', `ts` = %s"
        cursor = cnx.cursor()
        cursor.execute(sql, (cookies, proxy, ua, -1, status, info, int(time.time())))
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
        cursor.execute(sql, ('异常-data_security:%s' % status, int(time.time()), proxy_id, proxy_id))
        cnx.commit()

        cursor.close()
        cnx.close()

    def _merge_cookies(self, request_cookies, response_cookies):
        """
        合并request.cookies和response.cookies
        :params request_cookies: bytes
        :params response_cookies: list(bytes)
        :return string
        """
        req_cookies = ''
        rsp_cookies = ''
        if response_cookies:
            rsp_cookies = [c.decode('utf8').split(';', 1)[0] for c in response_cookies if c]
            rsp_cookies = "; ".join(rsp_cookies)
        if request_cookies:
            req_cookies = request_cookies.decode('utf8')
            req_cookies = req_cookies + '; '
        return req_cookies + rsp_cookies


class ValidateGDResponseMiddleware(object):
    """验证高德返回的结果"""
    def __init__(self, crawler, cnxpool, max_cookies_counter, proxies):
        self.crawler = crawler
        self.cnxpool = cnxpool
        self.max_cookies_counter = max_cookies_counter
        self.proxies = proxies

    def __del__(self):
        del self.cnxpool

    def process_request(self, request, spider):
        pass

    def process_response(self, request, response, spider):
        cookies_id = request.meta['cookies_id']        
        cookies_counter = request.meta['cookies_counter']

        # 防止同一代理之间使用频率过快
        time.sleep(5)
        
        # 从集合中释放proxy
        if 'proxy' in request.meta and request.meta['proxy'] in self.proxies:
            self.proxies.remove(request.meta['proxy'])

        cookies_counter += 1

        url = response.url
        query = urllib.parse.urlsplit(response.url).query

        # 获取cookies
        request_cookies = request.headers.get('Cookie')
        response_cookies = response.headers.getlist('Set-Cookie')
        cookies = self._merge_cookies(request_cookies, response_cookies)
        logger.info('After response cookies: %s' % cookies)
        

        try:
            response_dict = json.loads(response.text)

            if 'status' not in response_dict or response_dict['status'] == '110':
                # 当response_dict['status'] 等于 101时，说明高德已经检测到抓取行为，
                # 该代理可能已经被检测到，冻结该代理，并将当前请求重新加入任务队列

                # 记录发生异常的请求信息
                err_info = ''
                if 'status' not in response_dict:
                    err_info = 'result not in response_dict'
                else:
                    err_info = response_dict['status']

                # 发生异常时，通过设置cookies的使用次数，使该cookies失效
                cookies_counter = self.max_cookies_counter + 100
                self.update_cookies(cookies_id, cookies, cookies_counter, 
                    query, err_info)

                logger.error(response.text)

                # 冻结代理
                # self.block_proxy(proxy_id, data_security)

                # 设置响应码为601，进行Retry
                response.status = 601
                return response

            elif response_dict['status'] != '1':
                # 记录发生异常的请求信息                
                self.update_cookies(cookies_id, cookies, cookies_counter, 
                    query, response.text)
                logger.debug('cookies_id: %s\t cookies:%s\t cookies_counter:%s' % 
                    (cookies_id, cookies, cookies_counter))
                logger.error(response.text)
                # 设置响应码为602，进行Retry
                response.status = 602
                return response

            else:
                # 更新cookies信息
                self.update_cookies(cookies_id, cookies, cookies_counter, query, response_dict['status'])
                return response

        except Exception as e:
            logger.error('proxy: %s\nua: %s\ncookies: %s\nquery: %s\n' % 
                (request.meta['proxy'], request.headers['User-Agent'], cookies, query), 
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

        proxies = settings['PROXIES']

        return cls(crawler, cnxpool, max_cookies_counter, proxies)

    def update_cookies(self, cookies_id, cookies, cookies_counter, info, status=None):
        """
        更新数据库中的cookies信息
        :params cookies_id: cookies_id
        :params cookies: cookies string
        :params cookies_counter: proxy-ua的使用次数
        :params info: 相关信息（url中的query信息）
        :params status: cookies状态（0: 正常，otherwise: 异常）
        """
        cnx = self.cnxpool.get_connection()
        # cookies = None

        
        sql = ("""UPDATE `cookies` SET `cookies` = %s, `counter` = %s, `status` = %s """
                """, `info` = %s, `ts` = %s WHERE `id` = %s""")
        cursor = cnx.cursor()
        cursor.execute(sql, (cookies, cookies_counter, status, info, int(time.time()), cookies_id))
        cnx.commit()

        cursor.close()
        cnx.close()

    def insert_cookies(self, proxy, cookies, ua, info, status):
        """
        将新的cookies信息插入数据库
        :params proxy: cookies_id
        :params cookies: cookies string
        :params ua: ua
        :params info: 相关信息（url中的query信息）
        :params status: cookies状态（0: 正常，otherwise: 异常）
        """
        cnx = self.cnxpool.get_connection()
        sql = "INSERT INTO `cookies` SET `cookies` = %s, `proxy` = %s, `ua` = %s, `counter` = %s, `status` = %s, `info` = %s, `web` = \'baidu_map\', `ts` = %s"
        cursor = cnx.cursor()
        cursor.execute(sql, (cookies, proxy, ua, -1, status, info, int(time.time())))
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
        cursor.execute(sql, ('异常-data_security:%s' % status, int(time.time()), proxy_id, proxy_id))
        cnx.commit()

        cursor.close()
        cnx.close()

    def _merge_cookies(self, request_cookies, response_cookies):
        """
        合并request.cookies和response.cookies
        :params request_cookies: bytes
        :params response_cookies: list(bytes)
        :return string
        """
        req_cookies = ''
        rsp_cookies = ''
        if response_cookies:
            rsp_cookies = [c.decode('utf8').split(';', 1)[0] for c in response_cookies if c]
            rsp_cookies = "; ".join(rsp_cookies)
        if request_cookies:
            req_cookies = request_cookies.decode('utf8')
            req_cookies = req_cookies + '; '
        return req_cookies + rsp_cookies