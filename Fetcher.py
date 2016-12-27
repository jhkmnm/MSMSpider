#coding=utf-8

from __future__ import print_function
import socket
import re
import logging as log
import chardet
import urllib2
import StringIO
import gzip
import cookielib
import threading
from lxml import etree
import time

class Fetcher:
    """
    抓取具体内容
    """
    def __init__(self, url, output, timeout, cookie):
        self.url = url
        self.output_dir = output
        self.timeout = timeout
        self.cookie = cookie
        self.datefmt = "%Y-%m-%d %H:%M:%S"
        self.pcode = ""

    def check_url(self, url):
        """
        检查页面地址是否符合规范
        :param url: 待检查的地址
        :return: True(valid) / False(invalid)
        """
        url_format = '(http|ftp|https):\/\/[\w\-_]+(\.[\w\-_]+)+([\w\-\.,@?^=%&amp;:/~\+#]*[\w\-\@?^=%&amp;/~\+#])?'
        url_pattern = re.compile(url_format)
        if url_pattern.match(url):
            return True
        return False

    def read_content(self):
        """
        读取页面内容
        :return: content (string)
        """
        print(threading.current_thread().name + ":%s - getting url: %s" % (time.strftime(self.datefmt, time.localtime()), self.url))
        content = ''

        try:
            opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookielib.CookieJar()))
            urllib2.install_opener(opener)
            request = urllib2.Request(self.url)
            request.add_header("Cookie", self.cookie)
            response = urllib2.urlopen(request, timeout = self.timeout)
            if response.info().get('Content-Encoding',"") == 'gzip':
                buf = StringIO.StringIO(response.read())
                f = gzip.GzipFile(fileobj=buf)
                content = f.read()
            else:
                content = response.read()
            print(threading.current_thread().name + ":%s - geted url: %s" % (time.strftime(self.datefmt, time.localtime()), self.url))
            return content
        except socket.timeout:
            log.warn("Timeout in fetching %s" % self.url)
        except urllib2.HTTPError as e:
            log.warn("error in fetching content of %s: %s" % (self.url, e))
            return content
        except urllib2.URLError as e:
            log.warn("error in fetching content of %s: %s" % (self.url, e))
            return content
        except socket.gaierror as e:
            log.warn("error in fetching content of %s: %s" % (self.url, e))
            return content
        except Exception as e:
            log.warn("error in fetching content of %s: %s" % (self.url, e))
            return content

    def get_sub_urls(self, content):
        """
        获取子类地址
        :param content: 当前页面内容(分类页面)
        :return: Url List
        """

        if not self.check_url(self.url):
            return []
        sub_urls = []

        log.info(threading.current_thread().name + " - getting url: %s" % self.url)
        tree = etree.HTML(content.decode("utf-8"))

        classpath = "//*[@id='wrapper']/div[5]/div[4]/div[1]/div/ul/li"

        """
        子类会有多个级别，只需要取最后一级子类
        xxx/11000001/
        xxx/11000001/1100001111/   末级
        """
        secondcount = len(tree.xpath(classpath))
        for i in range(1, secondcount + 1):
            urls = tree.xpath(classpath + "["+ str(i) +"]/ul/li//a/@href")
            for url in urls:
                isend = 1
                for u in urls:
                    if url in u and url != u:
                        isend = 0
                        break
                if isend == 1:
                    sub_urls.append('http://cn.misumi-ec.com' + url)
        return sub_urls

    def get_product_url(self, content):
        """
        获取产品详情地址
        :param content: 当前页面内容(产品列表页)
        :return: Url List
        """
        if not content:
            return []

        try:
            tree = etree.HTML(content.decode("utf-8"))
            isproduct = len(tree.xpath("//div[@class='selectBox__title']"))
            if isproduct == 0:
                return []

            log.info(threading.current_thread().name + ":%s - getting url: %s" % (time.strftime(self.datefmt, time.localtime()), self.url))

            product_urls = []
            urls = tree.xpath("//*[starts-with(@id, 'List')]/@href")
            print (threading.current_thread().name + " product_urls " + str(len(urls)))
            [product_urls.append('http://cn.misumi-ec.com' + url) for url in urls]

            next_page = tree.xpath("//*[@id='search_pager_upper_right']/a/@href")

            # 列表页面有分页
            if next_page:
                print(threading.current_thread().name + " next_page " + next_page[0])
                self.url = self.url.split('?')[0] + next_page[0]
                content = self.read_content()
                for item in self.get_product_url(content):
                    product_urls.append(item)

            return product_urls
        except Exception as e:
            log.warn("error in fetching content of %s: %s" % (self.url, e))

    def get_product_info(self, content):
        """
        产品详情信息
        :param content: 当前页面内容
        :return: [{key:value}]
        """
        if not content:
            return []

        log.info(threading.current_thread().name + ":%s - getting url: %s" % (time.strftime(self.datefmt, time.localtime()), self.url))

        try:
            tree = etree.HTML(content.decode("utf-8"))
            product_infos = []
            title = tree.xpath("//*[@id='wrapper']/div[5]/ul[1]/li[2]/a/span/text()")[0]
            trs = tree.xpath("//*[@id='ListTable']/tr")

            codepath_link = "td[@class='model']/div/a/span/span/text()"
            codepath_nolink = "td[@class='model']/div"

            if trs:
                for tr in trs:
                    code = tr.xpath(codepath_link)

                    if code:
                        shipday = tr.xpath("td[@class='shipDay']/span")[0].text
                        dic = {"title": title, "url" : self.url, "code" : "".join(code), "shipday" : shipday}
                        product_infos.append(dic)
                    else:
                        code = tr.xpath(codepath_nolink)
                        if code:
                            for item in code:
                                self.tryFindChild(item)

                            shipday = tr.xpath("td[@class='shipDay']/span")[0].text
                            dic = {"title": title, "url": self.url, "code": self.pcode, "shipday": shipday}
                            product_infos.append(dic)

                next_page = tree.xpath("//*[@id='detail_codeList_pager_upper_right']/a/@href")

                if next_page:
                    self.url = self.url.split('?')[0] + next_page[0]
                    content = self.read_content()
                    for item in self.get_product_info(content):
                        product_infos.append(item)
            else:
                divs = tree.xpath("//div[@class='productList__table']")
                for div in divs:
                    code = div.xpath("div/div/div/div/a/text()")
                    if code:
                        shipday = div.xpath("div/div/div[@class='td--inner']/text()")
                        dic = {"title": title, "url": self.url, "code": code, "shipday": shipday}
                        product_infos.append(dic)

            if not product_infos:
                print(1)

            return product_infos
        except Exception as e:
            log.warn("error in fetching content of %s: %s" % (self.url, e))

    def tryFindChild(self, element):
        children = element.getchildren()
        if len(children):
            for item in children:
                self.tryFindChild(item)
        else:
            self.pcode += (element.text if element.text != None else "")