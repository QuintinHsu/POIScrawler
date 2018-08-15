# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


# class PoiscrawlerItem(scrapy.Item):
class PoiItem(scrapy.Item):
    # define the fields for your item here like:
    tbl = scrapy.Field()
    uid = scrapy.Field()
    raw = scrapy.Field()
    ts = scrapy.Field()