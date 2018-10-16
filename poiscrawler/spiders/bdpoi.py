# -*- coding: utf-8 -*-
import time
import json
import random

import scrapy
import urllib.parse

from poiscrawler.items import *


class BdpoiSpider(scrapy.Spider):
    name = 'bdpoi'
    allowed_domains = ['map.baidu.com']
    start_url = 'https://map.baidu.com/'

    custom_settings = {
        # 'JOB_DIR': './baidu_job_1',

        'DEFAULT_REQUEST_HEADERS': {
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        },

        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': None,
            'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': None,
            'poiscrawler.myMiddlewares.proxy_ua_cookies.RandomProxyUACookiesMiddleware': 543
        },

        'ITEM_PIPELINES': {
           'poiscrawler.pipelines.InsertManyItemPipeline': 300
        },

        'DOWNLOAD_DELAY': 10,
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_TIMEOUT': 10,

        # Retry
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 10,
        'RETRY_HTTP_CODES': [400, 404, 502, 504, 601, 602],

        # MySQL
        'MYSQL_ITEM_LIST_LIMIT': 50,

        'SUBBINS': 5,
        'SUBBIN_PADDING': 1
    }

    params = {
        'newmap': 1,
        'reqflag': 'pcmap',
        'biz': 1,
        'from': 'webmap',
        'da_par': 'direct',
        'pcevaname': 'pc4.1',
        'qt': 'spot',
        'from': 'webmap',
        # 'c': 131,
        'wd': '美食',
        'wd2': '',
        'pn': 0,
        'nn': 0,
        'db': 0,
        'sug': 0,
        'addr': 0,

        'pl_data_type': 'scope',  # 'cater',
        'pl_sub_type': '',
        'pl_price_section': '0+',
        'pl_sort_type': 'data_type',
        'pl_sort_rule': 0,
        'pl_discount2_section': '0,+',
        'pl_groupon_section': '0,+',
        'pl_cater_book_pc_section': '0,+',
        'pl_hotel_book_pc_section': '0,+',
        'pl_ticket_book_flag_section': '0,+',
        'pl_movie_book_section': '0,+',
        'pl_business_type': 'scope',  # 'cater',
        'pl_business_id': '',
        'u_loc': '12931406,4885246',
        'ie': 'utf-8',

        'da_src': 'pcmappg.poi.page',
        'on_gel': 1,
        'src': 7,
        'gr': 3,
        'l': 21,  # 12.639712187500002
        'rn': 50,
        'tn': 'B_NORMAL_MAP',
        'b': '(12925857.482151808,4827933.7494841525;12981969.788073862,4854018.166951579)',
        't': 1529135387071
    }

    def start_requests(self):        
        # 设置请求参数
        keys = list()
        with open('./data/baidu_keywords') as f:
            key = f.readline().replace('\n', '')
            if key:
                keys.append(key)
        for key in keys:
            url = self._construct_url(key, '(12575076.8056,2564378.2789;12697994.1393,2728618.8495)', 0)
            yield scrapy.Request(url=url, method='GET')

    def parse(self, response):

        # self.logger.debug(response.text)

        response_dict = json.loads(response.text)
        ts = int(time.time())

        # 获取请求的查询条件
        query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(response.url).query))

        # 该区域有POI数据
        if 'content' in response_dict and response_dict['content']:
            poi_total = int(response_dict['result']['total'])

            # 只有1页数据，直接将该POI数据返回
            if poi_total > 0 and poi_total <= 50:  
                self.logger.info("返回第一页的POI数据（只有一页数据）, wd:%s, bound:%s, nn:%s, total:%s" % 
                    (query['wd'], query['b'], query['nn'], poi_total))

                for poi in response_dict['content']:
                    yield self.parse_poi(poi, ts)

            elif poi_total > 50 and poi_total < 760:  # 有多页数据
                # 剩余多少数据（含本次返回的数据）
                remain_poi_num = poi_total - int(query['nn'])

                # 本次请求返回的数据是最后一页数据，直接将该POI数据返回
                if remain_poi_num > 0 and remain_poi_num <= 50:  
                    logger.info('返回最后一页的POI数据, wd:%s, bound:%s, nn:%s, total:%s' % (
                        query['wd'], query['b'], query['nn'], poi_total))
                    for poi in response_dict['content']:
                        yield self.parse_poi(poi, ts)

                # 构造下一页数据的请求，并将该请求加入的任务队列
                elif remain_poi_num > 50:
                    self.logger.info('返回当前页的POI数据，请求下一页数据, wd:%s, bound:%s, nn:%s, total:%s, remain: %s' % (
                        query['wd'], query['b'], query['nn'], poi_total, remain_poi_num))

                    # 保存当前页的数据
                    for poi in response_dict['content']:
                        yield self.parse_poi(poi, ts)

                    # 构造下一页请求
                    next_url = self._construct_url(query['wd'], query['b'], int(query['nn']) + 50)                    
                    yield scrapy.Request(url=next_url, method='GET')

            # 百度在某个区域最多只能搜索到760个POI，如果poi_total=760，则对该区域进行分割，
            # 并构造每个子区域的请求
            elif poi_total >= 760:
                self.logger.info('对bound进行分割，wd:%s, bound:%s, nn:%s, total:%s' % (
                    query['wd'], query['b'], query['nn'], poi_total))

                # 分割区域，构造子区域的请求
                sub_area_bounds = self._calc_subarea(query['b'], self.custom_settings['SUBBINS'],
                    self.custom_settings['SUBBIN_PADDING'])

                for sub_area_bound in sub_area_bounds:
                    next_url = self._construct_url(query['wd'], sub_area_bound, 0)                    
                    yield scrapy.Request(url=next_url, method='GET')
        # 该区域没有有POI数据
        else:
            pass

    def parse_poi(self, poi, ts):
        """
        将百度返回的content转换成Item
        :param poi: 百度返回的poi
        :param ts: 时间戳
        :return Item: 返回POIItem
        """
        poi_item = PoiItem()
        if 'uid' in poi and poi['uid'] and poi['uid'] != 'null':
            poi_item['tbl'] = 'bd_poi'
            poi_item['uid'] = poi['uid']
            poi_item['raw'] = json.dumps(poi)
            poi_item['ts'] = ts
        return poi_item

    def _construct_url(self, wd, bound, nn):
        """
        构造请求参数
        :param wd:      搜索关键词
        :param bound:   搜索区域
        :param nn:      poi数据的起始索引（百度每页最多只有50条数据，假设搜索结果中有200个poi，
                        若nn=30，则本次请求返回第30-79这50个poi数据）
        :return:        构造的请求参数
        """
        params = self.params
        params['t'] = '{:.0f}'.format(time.time() * 1000)
        params['wd'] = wd
        params['b'] = bound
        params['nn'] = nn
        # 随机选取地理位置
        params['u_loc'] = '%s,%s' % (random.randint(
            12834000, 13091000), random.randint(4720000, 5006000))

        return self.start_url + "?" + urllib.parse.urlencode(params)

    def _calc_subarea(self, bound, partition_num=5, padding=1):
        """
        对搜索区域进行分割,分割成 partition_num * partition_num 个子区域
        :param bound:               被分割的区域边界
        :param partition_num:       长、宽等分成partition_num个
        :param padding:             长、宽扩展量
        :return:                    分割后的各子区域的边界列表
        """
        area_bound = bound[1:-1].split(";")

        # 左下角地理坐标
        lb = list(map(float, area_bound[0].split(',')))
        # 右上角地理坐标
        rt = list(map(float, area_bound[1].split(',')))

        # 计算每个子区域的长与宽
        x_bandwidth = (rt[0] - lb[0]) / partition_num
        y_bandwidth = (rt[1] - lb[1]) / partition_num

        # 向上、向右进行扩展，防止由于坐标变形而导致每个区域之间有缝隙
        # x_padding = padding if x_bandwidth / 2 > padding else x_bandwidth / 2
        # y_padding = padding if y_bandwidth / 2 > padding else y_bandwidth / 2
        x_padding = padding
        y_padding = padding

        # 返回各个子区域的bound
        for i in range(partition_num):
            for j in range(partition_num):
                sub_lb_0 = lb[0] + i * x_bandwidth
                sub_lb_1 = lb[1] + j * y_bandwidth

                sub_rt_0 = lb[0] + (i+1) * x_bandwidth + x_padding
                sub_rt_1 = lb[1] + (j+1) * y_bandwidth + y_padding

                sub_area_bound = "(%s,%s;%s,%s)" % (
                    sub_lb_0, sub_lb_1, sub_rt_0, sub_rt_1)
                yield sub_area_bound