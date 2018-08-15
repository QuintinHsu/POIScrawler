# -*- coding: utf-8 -*-
import logging
from collections import defaultdict

from twisted.enterprise import adbapi
import mysql.connector
from mysql.connector.errors import OperationalError, InterfaceError


# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

logger = logging.getLogger(__name__)

class PoiscrawlerPipeline(object):
    def process_item(self, item, spider):
        return item


class InsertManyPipeline(object):
    def __init__(self, spider, settings):
        self.spider = spider

        self.dbpool = adbapi.ConnectionPool('mysql.connector',
                host = settings.get('MYSQL_HOST', '127.0.0.1'),
                port = settings.get('MYSQL_PORT', 3306),
                user = settings.get('MYSQL_USER', 'username'),
                passwd = settings.get('MYSQL_PASSWD', 'password'),
                db = settings.get('MYSQL_DBNAME', 'poi_db')
            )

        self.mysql_reconnect_wait = settings.get('MYSQL_RECONNECT_WAIT', 3)
        self.mysql_item_list_limit = settings.get('MYSQL_ITEM_LIST_LIMIT', 50)
        # self.item_list = list()

        self.item_dict = defaultdict(list)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            spider = crawler.spider,
            settings = crawler.settings
        )

    def close_spider(self, spider):
        """
        spider关闭前，批量保存item_list中的数据
        """
        for k, v in self.item_dict.items():
            if len(v) > 0:
                self._insertmany(list(v), tbl=k)

    def process_item(self, item, spider):
        if item:
            item_key = item.pop('tbl', None)
            self.item_dict[item_key].append(dict(item))
            # self.item_list.append(dict(item))
            # self.item_list.append({'raw': 'hehe', 'uid':'111', 'ts': 1111})

            if len(self.item_dict[item_key]) >= self.mysql_item_list_limit:
                logger.debug('0 len of item_list: %s' % len(self.item_dict[item_key]))
                logger.debug('0 table: %s' % item_key)
                self._insertmany(list(self.item_dict[item_key]), tbl=item_key)
                self.item_dict[item_key].clear()

        return item

    def sql(self, txn, item_list, tbl):
        """
        批量插入的执行语句
        :param txn: 事务
        :param item_list: 待批量插入的数据
        :param tbl: 数据库表名
        """
        raise NotImplementedError('Subclass of InsertManeyPipeline must implement the sql() method')

    def _insertmany(self, item_list, tbl='bd_poi', retrying=True):
        """
        使用twisted异步调用insertmany
        :param item_list: 待批量插入的数据
        :param tbl: 数据库表名
        :param retrying: 发生异常后是否重试
        """
        logger.debug('1 len of item_list: %s' % len(item_list))
        logger.debug('1 table: %s' % tbl)
        d = self.dbpool.runInteraction(self.sql, item_list, tbl)
        d.addCallback(self.handle_result, item_list, tbl)
        d.addErrback(self.handle_error, item_list, tbl, retrying)

    def handle_result(self, result, item_list, tbl):
        """
        处理批量插入后的执行结果
        :param result: 批量插入后的执行结果
        :param item_list: 已批量插入的数据
        :param tbl: 数据库表名
        """
        logger.info('%s\t%s items inserted with return code %s' % (tbl, len(item_list), result))

    def handle_error(self, result, item_list, tbl, retrying=False):
        """
        处理批量插入时出现的异常
        :param result: 批量插入时出现的异常
        :param item_list: 已批量插入的数据
        :param tbl: 数据库表名
        :param retrying: 出错是否重试
        """
        args = result.value.args

        # 出现OperationalError, InterfaceError异常时，进行重试
        # <class 'pymysql.err.OperationalError'> (1045, "Access denied for user 'username'@'localhost' (using password: YES)")
        # <class 'pymysql.err.OperationalError'> (2013, 'Lost connection to MySQL server during query ([Errno 110] Connection timed out)')
        # <class 'pymysql.err.OperationalError'> (2003, "Can't connect to MySQL server on '127.0.0.1' ([WinError 10061] 由于目标计算机积极拒绝，无法连接。)")
        # <class 'pymysql.err.InterfaceError'> (0, '')    # after crawl started: sudo service mysqld stop
        if result.type in [OperationalError, InterfaceError]:
            if not retrying:
                logger.info('MySQL: exception {} {} {} {}'.format(
                                        result.type, args, tbl, len(item_list)))            
                self.spider.logger.debug('MySQL: Trying to recommit in %s sec' % self.mysql_reconnect_wait)
                
                # https://twistedmatrix.com/documents/12.1.0/core/howto/time.html
                from twisted.internet import task
                from twisted.internet import reactor
                task.deferLater(reactor, self.mysql_reconnect_wait, self._insertmany, item_list, tbl, True)
            else:
                logger.warn('MySQL: exception {} {} {} {}'.format(
                                        result.type, args, tbl, len(item_list)))

            return

        # 打印未知异常信息
        else:
            logger.error('MySQL: {} {} unhandled exception from item_list: {} {}'.format(
                                result.type, args, tbl, len(item_list)))

            return

class InsertManyItemPipeline(InsertManyPipeline):
    """docstring for ClassName"""
    def sql(self, txn, item_list, tbl):
        logger.debug('2 len of item_list: %s' % len(item_list))
        insert_sql = "INSERT INTO `"+ tbl +"`(`uid`, `raw`, `ts`) VALUES (%(uid)s, COMPRESS(%(raw)s), %(ts)s) ON DUPLICATE KEY UPDATE `raw`=VALUES(`raw`),`ts`=VALUES(`ts`)"
        return txn.executemany(insert_sql, item_list)
        
