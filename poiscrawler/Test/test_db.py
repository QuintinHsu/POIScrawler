import time
import json
import random
import mysql.connector.pooling


def create_connection_pool():
    dbconfig = dict(
        host = '127.0.0.1',#读取settings中的配置
        db = 'poi_db',
        user = 'root',
        passwd = '12345678',
    )
    cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name = "pucpool",
                                                      pool_size = 1, **dbconfig)
    return cnxpool

def test_insert_proxies(cnxpool):
    cnx = cnxpool.get_connection()

    sql = ('INSERT INTO `proxies` (`proxy`, `status`, `ts`, `counter`, `web`) '
            'VALUES (%s, %s, %s, %s, %s)')
    cursor = cnx.cursor()
    cursor.execute(sql, ('171.42.132.167:10000', '0', int(time.time()), 0, 'gaode_map'))
    cnx.commit()

    cursor.close()
    cnx.close()

def test_insert_cookies(cnxpool):
    cnx = cnxpool.get_connection()
    
    cursor = cnx.cursor()
    sql = 'SELECT `id`, `proxy`, `web` FROM `proxies` WHERE web = \'gaode_map\''
    cursor.execute(sql)
    proxy = cursor.fetchone()
    # cursor.close()

    sql = 'SELECT `id`, `ua` FROM `ua`'
    cursor.execute(sql)
    uas = cursor.fetchall()

    cookies = list()
    for ua in uas:
        cookies.append([proxy[0], proxy[1], proxy[2], ua[0], ua[1]])

    sql = ('INSERT INTO `cookies` (`proxy_id`, `proxy`, `web`, `ua_id`, `ua`) '
            'VALUES (%s, %s, %s, %s, %s)')
    # 打乱cookies
    random.shuffle(cookies)
    cursor.executemany(sql, cookies)
    cnx.commit()

    cursor.close()
    cnx.close()
    # print(proxy)

def test_insert_ua(cnxpool, uas):
    cnx = cnxpool.get_connection()
    sql = 'INSERT INTO `ua` (`ua`, `browser`, `status`, `ts`) VALUES (%s, %s, %s, %s)'
    cursor = cnx.cursor()
    cursor.executemany(sql, uas)
    cnx.commit()

    cursor.close()
    cnx.close()

def test_select_null(cnxpool):
    cnx = cnxpool.get_connection()
    cursor = cnx.cursor()
    sql = 'SELECT * FROM cookies WHERE `web` = \'1\' LIMIT 1'
    cursor.execute(sql)
    cookies = cursor.fetchone()
    cursor.close()
    cnx.close()

    print(cookies)


if __name__ == '__main__':
    uas = list()
    with open('ua.json', 'r') as f:
        uas_json = json.load(f)
        for k, v in uas_json['browsers'].items():
            for u in v:
                uas.append([u, k, '0', int(time.time())])

    cnxpool = create_connection_pool()
    # test_insert_proxies(cnxpool)
    # test_insert_ua(cnxpool, uas)
    test_insert_cookies(cnxpool)
    # test_select_null(cnxpool)

    del cnxpool
    # print(random.randint(1,1))