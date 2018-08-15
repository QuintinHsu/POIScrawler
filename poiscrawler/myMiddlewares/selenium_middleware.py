
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

logger = logging.getLogger(__name__)

class SeleniumChromeMiddleware():
    """docstring for SeleniumChromeMiddleware"f __init__(self, arg):"""
        def __init__(self, timeout=None, service_args=[]):
            self.timeout = timeout
            self.browser = webdriver.Chrome()
            self.browser.set_window_size(1400, 700)
            self.browser.set_page_load_timeout(self.timeout)
            self.wait = WebDriverWait(self.browser, self.timeout)
            self.chrome_options = Options()
            self.chrome_options.add_argument('--headless')

        def __del__(self):
            self.browser.close()

        def process_request(self, request, spider):
            logger.debug('Chrome is starting')
            ua = request.meta.get('ua', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36')
            
            # 设置ua
            url = 'https://www.amap.com'
            self.chrome_options.add_argument('--user-agent=%s' % ua)
            self.browser.get(url=url)
            pass

        def from_crawler(cls, crawler):
            return cls(timeout=crawler.settings['SELENIUM_TIMEOUT']，
                service_args=crawler.settings['CHROME_SERVICE_ARGS'])
        