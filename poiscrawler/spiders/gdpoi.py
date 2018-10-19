# -*- coding: utf-8 -*-
import time
import json
import random

import scrapy
import urllib.parse

from poiscrawler.items import *


class GdpoiSpider(scrapy.Spider):
    name = 'gdpoi'
    allowed_domains = ['ditu.amap.com', 'www.amap.com']
    start_url = 'https://ditu.amap.com/service/poiInfo/'

    custom_settings = {
        'DEFAULT_REQUEST_HEADERS': {
            'upgrade-insecure-requests': 1,
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8'
        },

        'DOWNLOADER_MIDDLEWARES': {
            # 'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            # 'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': None,
            # 'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': None,
            'poiscrawler.myMiddlewares.proxy_ua_cookies.RandomProxyUACookiesMiddleware': 443,
            'poiscrawler.myMiddlewares.validate_response.ValidateGDResponseMiddleware': 553
        },

        'ITEM_PIPELINES': {
           'poiscrawler.pipelines.InsertManyItemPipeline': 300
        },

        'COOKIES_ENABLED': True,
        #'COOKIES_DEBUG': True,

        'DOWNLOAD_DELAY': 10,
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_TIMEOUT': 10,

        # Retry
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 1,
        'RETRY_HTTP_CODES': [400, 404, 502, 504, 601, 602],

        # MySQL
        'MYSQL_ITEM_LIST_LIMIT': 50,

        'SUBBINS': 5,
        'SUBBIN_PADDING': 1,

        # PAGE_POI_NUM
        'PAGE_POI_NUM': 200,

        #'LOG_LEVEL': 'INFO',
        #'LOG_FILE': './gdpoi.log',

        # 正在使用的proxy
        'PROXIES': [],

        'WEB': 'gaode_map'
    }

    params = {
        'query_type': 'TQUERY',
        'pagesize': 30, # 最大为30
        'pagenum': 1, # 从1开始
        'qii': 'true',
        'cluster_state': 5,
        'need_utd': 'false',
        'utd_sceneid': 1000,
        'div': 'PC1000',
        'addr_poi_merge': 'true',
        'is_classify': 'true',
        'zoom': 18,
        'city': 110000,
        'geoobj': '115.281974|39.172454|117.590798|41.142945',
        '_src': 'around',
        'SPQ': 'true',
        'keywords': '美食'
    }

    def start_requests(self):        
        # 设置请求参数     
        # url = self._construct_url('美食', '115.281974|39.172454|117.590798|41.142945', 110000, 0)
        # url = self._construct_url('美食', '116.3690332|40.5606504|116.390898|40.7548486', 110000, 0)
        # yield scrapy.Request(url=url, method='GET')
        keys = list()
        with open('./poiscrawler/data/baidu_keywords', 'r', encoding='utf8') as f:
            lines = f.readlines()
            for l in lines:
                key = l.replace('\n', '')
                if key:
                    keys.append(key)
        for key in keys:
            url = self._construct_url(key, '120.8430290|30.4360185|122.0540010|31.8981599', 310000, 0)
            yield scrapy.Request(url=url, method='GET')
        

    def parse(self, response):

        response_dict = json.loads(response.text)
        ts = int(time.time())

        # 获取请求的查询条件
        query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(response.url).query))

        if response_dict['data']['result'] == 'true' and int(response_dict['data']['total']) > 0:
            page_num = int(response_dict['searchOpt']['pageIndex'])
            page_size = int(response_dict['searchOpt']['pageSize'])
            poi_total = int(response_dict['data']['total'])

            bound = query['geoobj']

            area_bound = list(map(float, bound.split("|")))

            # 左下角地理坐标
            lb = area_bound[0:2]
            # 右上角地理坐标
            rt = area_bound[2:4]

            #在某个区域搜索POI大于200个,或者区域长宽约大于10米，则对该区域进行分割，并构造每个子区域的请求
            if poi_total > 200 and rt[0] - lb[0] > 0.0001 and rt[1] - lb[1] > 0.0001:                             
                self.logger.info('对bound进行分割，keywords:%s, bound:%s, page_num:%s, total:%s' % (query['keywords'], 
                    query['geoobj'], query['pagenum'], poi_total))
                sub_area_bounds = self._calc_subarea(query['geoobj'])
                for sub_area_bound in sub_area_bounds:
                    next_url = self._construct_url(query['keywords'], sub_area_bound, query['city'], 1)
                    yield scrapy.Request(url=next_url, method='GET')
            else:
                poi_list = response_dict['data']['poi_list']
                # 当前返回的数据个数小于page_size，则说明是最后一页数据
                if len(poi_list) < 30: 
                    self.logger.info('返回最后一页POI数据，keywords:%s, bound:%s, page_num:%s, total:%s' % (query['keywords'], 
                        query['geoobj'], query['pagenum'], poi_total))
                    for poi in poi_list:
                        yield self.parse_poi(poi, ts)
                else:                                
                    self.logger.info('返回当前页的POI数据，请求下一页数据，keywords:%s, bound:%s, page_num:%s, total:%s' % (
                        query['keywords'], query['geoobj'], query['pagenum'], poi_total))
                    next_url = self._construct_url(query['keywords'], query['geoobj'], query['city'], page_num + 1)
                    yield scrapy.Request(url=next_url, method='GET')

                    for poi in poi_list:
                        yield self.parse_poi(poi, ts)

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
        if 'id' in poi and poi['id'] and poi['id'] != 'null':
            poi_item['tbl'] = 'gd_poi'
            poi_item['uid'] = poi['id']
            poi_item['raw'] = json.dumps(poi)
            poi_item['ts'] = ts
        return poi_item

    def _construct_url(self, wd, bound, city, pagenum):
        """
        构造请求参数
        :param wd:      搜索关键词
        :param bound:   搜索区域
        :param bound:   搜索城市代码
        :param pagenum: 请求页码
        :return:        构造的请求参数
        """
        params = self.params
        params['keywords'] = wd
        params['geoobj'] = bound
        params['pagenum'] = pagenum
        params['city'] = city

        return self.start_url + "?" + urllib.parse.urlencode(params)

    def _calc_subarea(self, bound, partition_num=5, padding=0.0001):
        """
        对搜索区域进行分割,分割成 partition_num * partition_num 个子区域
        :param bound:               被分割的区域边界
        :param partition_num:       长、宽等分成partition_num个
        :param padding:             长、宽扩展量
        :return:                    分割后的各子区域的边界列表
        """
        area_bound = list(map(float, bound.split("|")))

        # 左下角地理坐标
        lb = area_bound[0:2]
        # 右上角地理坐标
        rt = area_bound[2:4]
        
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

                sub_area_bound = "%s|%s|%s|%s" % (sub_lb_0, sub_lb_1, sub_rt_0, sub_rt_1)
                yield sub_area_bound
