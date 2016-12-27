#coding=utf-8

from __future__ import with_statement
from ConfigParser import ConfigParser
import os
import logging
import Queue
import Fetcher
import logging as log
import threading
import re

class MSMSpider(object):
    def __init__(self, configure):
        self._url_list = configure.get("spider", "url_list_file")
        self._output_dir = configure.get("spider", "output_directory")  # 如果需要下载图片之类的，保存目录
        self._max_depth = int(configure.get("spider", "max_depth"))     # 钻取的深度，暂时没有使用
        self._timeout = int(configure.get("spider", "crawl_timeout"))   # 读取网页超时时间
        self.fetch_pattern = configure.get("spider", "target_url")      # 用于大类验证网址
        self.pro_pattern = configure.get("spider", "product_url")       # 产品详情页面网址
        self.fig_pattern = re.compile(self.fetch_pattern)
        self.pro_match = re.compile(self.pro_pattern)
        self._thread_count = int(configure.get("spider", "thread_count"))   # 线程数量
        self._cookie = configure.get("spider", "cookie")
        self._url_queue = Queue.Queue()
        self._url_visited = [] # 保存已经访问过的地址
        self._lock = threading.Lock()
        self.init_urls()
        if not os.path.exists(self._output_dir):
            os.makedirs(self._output_dir)

        self._thread_list = []

    def init_urls(self):
        """
        根据配置初始化需要访问的地址,添加到队列
        :return:None
        """
        init_depth = 1
        with open(self._url_list) as url_file:
            for line in url_file:
                line = line.strip()
                try:
                    if line not in self._url_visited:
                        self._url_queue.put([line, init_depth], timeout=1)
                        self._url_visited.append(line)
                except Queue.Full as e:
                    log.warn(e)
                    pass

    def fetch(self):
        """
        从队列中获取地址并查询内容
        :return: None
        """
        log.info("Running thread %s" % threading.current_thread().name)
        while True:
            try:
                cur_url, cur_depth = self._url_queue.get(timeout=1)
                print self._url_queue.unfinished_tasks
                cur_url = cur_url.strip()
            except Queue.Empty as e:
                log.warn(e)
                continue

            fetch_tool = Fetcher.Fetcher(cur_url,
                                         self._output_dir,
                                         self._timeout,
                                         self._cookie)

            if self.pro_match.match(cur_url):
                # 产品详情页
                self.save_product_info_url(cur_url)
                content = fetch_tool.read_content()
                info = fetch_tool.get_product_info(content)
                product_info = []
                [product_info.append(item) for item in info]
                if self._lock.acquire():
                    self._url_visited.append(cur_url)
                self._lock.release()

                self.save_product_info(product_info)
            elif self.fig_pattern.match(cur_url):
                # 产品大类
                content = fetch_tool.read_content()
                if content is None or len(content) == 0:
                    continue

                # 获取大类下的子类页面
                sub_urls = fetch_tool.get_sub_urls(content)
                if sub_urls is None:
                    continue
                for item in sub_urls:
                    if self._lock.acquire():  # lock _url_visited, check
                        if item in self._url_visited:
                            continue
                        self._lock.release()
                    try:
                        if self._lock.acquire():  # lock _url_visited, add
                            self._url_visited.append(item)
                        self._lock.release()
                        self._url_queue.put([item, cur_depth + 1], timeout=1)
                    except Queue.Full as e:
                        log.warn(e)
                        break
            else:
                # 子类页面， 获取产品详情地址
                content = fetch_tool.read_content()
                product_urls = fetch_tool.get_product_url(content)
                if product_urls is None:
                    continue
                for item in product_urls:
                    if self._lock.acquire():  # lock _url_visited, check
                        if item in self._url_visited:
                            self._lock.release()
                            continue
                        try:
                            self._url_visited.append(item)
                            self._url_queue.put([item, cur_depth + 1], timeout=1)
                        except Queue.Full as e:
                            log.warn(e)
                            break
                        finally:
                            self._lock.release()

            self._url_queue.task_done()

    def multi_thread(self):
        """
        启动线程
        :return:
        """
        for i in xrange(self._thread_count):
            single_thread = threading.Thread(target=self.fetch)
            self._thread_list.append(single_thread)
            single_thread.setDaemon(True)
            single_thread.start()

        # 等待所有任务的完成
        self._url_queue.join()

        log.info("mini spider finished")

    def save_product_info(self, product_info):
        """
        保存产品详情信息
        :return: None
        """
        if not product_info:
            return

        filename = "product_%s.txt" % product_info[0]["title"].encode("utf-8")
        fout = file(unicode(filename.replace("/","_"), 'utf-8'), "a")
        for info in product_info:
            try:
                fout.write("型式:" + info["code"].encode("utf-8") + " 货期:" + info["shipday"].encode("utf-8") + " 网址:" + info["url"] + "\n")
            except Exception as e:
                log.warn("error in save info of %s: %s" % (info["url"], e))
        fout.close()

    def save_product_info_url(self, url):
        """
        保存产品详情页地址，主要用于二次抓取时，不需要再从大数查找了
        :return: None
        """
        fout = file("product_info_url.txt", "a")
        fout.write(url + "\n")
        fout.close()

if __name__ == "__main__":
    config = ConfigParser()
    config.readfp(open("a.conf", 'r'))

    # 初始化日志
    LOG_LEVEL = logging.DEBUG
    logging.basicConfig(level=LOG_LEVEL,
            format="%(levelname)s:%(name)s:%(funcName)s->%(message)s",  #logging.BASIC_FORMAT,
            datefmt='%a, %d %b %Y %H:%M:%S', filename='spider.log', filemode='a')

    spider = MSMSpider(config)
    spider.multi_thread()