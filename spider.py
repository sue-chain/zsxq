#-*- coding:utf-8 -*-

import os
import time
import sys
import subprocess
import requests
import json
import logging
import random

import arrow

from selenium import webdriver

class Sleep(Exception):
    """睡眠"""
    pass

class NotGroupId(Exception):
    pass

class StopSpiderError(Exception):
    """停止抓取"""
    pass

class LoginTimeOut(Exception):
    pass

class Topic(object):
    """topic class"""

    pass


class BaseSpider(object):

    """BaseSpider. """

    def __init__(self, **kwargs):
        """init
        """
        self.session = requests.session()
        # 知识星球对User-Agent限制比较严格，估计对比了chrome具体版本, headers中的agent要写标准
        self.headers = {
            'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
        }
        self.token = ""
        self.topics = []
        self.comments = []
        self.pre_end_time = None    # topic列表(20条)中时间最早的topic create_time，取下20条topic的end_time参数
        self.stop_spider_time= arrow.get("2018-01-22")      # 定义爬取最早的记录时间，防止无限获取
        # 前一次爬取时间
        # 停止爬取时间，防止无限获取
        self.stop_spider_time = kwargs.get("stop_spider_time")
        self.group_id  = kwargs.get("group_id")
        self.base_data_path = "data"

    def execute(self):
        """execute spider"""
        try:
            self.init_browser()
            self.login_and_init_token()
            self.begin_spider()
        except KeyboardInterrupt as error:
            pass
        except Sleep as error:
            raise error
        except Exception as error:
            pass
        finally:
            self.cache_topics()
            self.cache_params()


    def init_browser(self):
        """init browser"""
        options = webdriver.ChromeOptions()
        prefs = {
            'profile.default_content_setting_values': {
                'images': 2
            }
        }
        # options.add_experimental_option('prefs', prefs)
        options.add_argument("User-Agent={}".format(self.headers["User-Agent"]))
        self.browser = webdriver.Chrome(chrome_options=options)
        self.browser.set_page_load_timeout(30)

    def login_and_init_token(self):
        """login_and_get_cookies
        """
        self.get_cache_token()
        if self.token:
            return

        login_url = "https://open.weixin.qq.com/connect/qrconnect?appid=wxa8d63c1238079ec4&response_type=code&scope=snsapi_login&redirect_uri=https%3A%2F%2Fwx.zsxq.com%2Fdweb%2F%23%2Fload&"
        #login_url = "https://wx.zsxq.com/dweb/"
        self.browser.get(login_url)
        while True:
            for cookie in self.browser.get_cookies():
                if cookie.get("value").find("access_token") < 0:
                    continue
                self.token = cookie.get("value").split("access_token")[1][3:]
                self.cache_token()
                return
            time.sleep(3)


    def begin_spider(self):
        """begin spider
        """
        if not group_id:
            raise NotGroupId("缺少group_id")
        while 1:
            topic_list = self.get_topic_list(self.group_id, self.pre_end_time)
            if not topic_list:
                raise StopSpiderError("没有帖子，停止抓取")

            # 检查topic_list时间最早的一条
            self.check_continue_spider(topic_list[-1])

            self.topics.extend(topic_list)

            for topic in topi_list:
                if topic["comments_count"] > len(topic["show_comments"]):
                    self.comments.extend(self.get_comment_list(topic))
                else:
                    self.comments.extend(topic["show_comments"])

            # 每1000条保存一次，防止数据过多崩溃
            if len(self.topics) % 1000 == 0:
                self.cache_topics()
            
            self.pre_spider_time = topic_list[-1]["create_time"]
            
            time.sleep(2)


    def get_topics_list(self, group_id=None, end_time=None):
        """获取topic_list
        topic = {
            topic_id: xx,
            type: talk,
            create_time: 创建时间,
            comments_count: 评论个数,
            show_comments: 随帖子展示出来的评论数，若comments_count>show_comments,需要获取全部评论
            likes_count: 点赞个数
            rewards_count: 赏金个数?
            talk: {
                owner: {
                    user_id: xx,
                    name: xx,
                    avatar_url
                },
                text: 话题内容, 有可能没有
                images: [{
                    thumbnail: {
                        url: 小图地址
                    },
                    large: {
                        url: 大图地址
                    },
                    image_id: 图片id
                }
                ]
            }

        }
        """
        params = {}
        url = "https://api.zsxq.com/v1.8/groups/{}/topics?count=20".format(group_id)
        if end_time:
            params = {"end_time": end_time}
        headers = self.headers.copy()
        headers["x-version"] = "1.8.8"
        headers["Accept"] = "*/*"
        headers["Connection"] = "keep-alive" 
        headers["Origin"] = "https://wx.zsxq.com"
        headers["authorization"] = self.token.encode("utf-8")
        headers["x-request-id"] = "bf192599-f8b6-c839-c629-dea24c41{}".format(random.randint(1000,1999))
        headers["Referer"] = "https://wx.zsxq.com/dweb/"
        headers["Accept-Encoding"] = "gzip, deflate, br"
        headers["Accept-Language"] = "zh-CN,zh;q=0.9,en;q=0.8"
        # response = requests.get(url, headers=headers, proxies={"https": "127.0.0.1:8081"})
        response = requests.get(url, headers=headers, params=params)
        result_json = response.json() 
        if not result_json["succeeded"]:
            raise LoginTimeOut("请重新登录")

        return result_json["resp_data"]["topics"]

    def get_comment_list(self, topic):
        """获取评论list"""
        params = {}
        topic_id = topic.get("topic_id")
        url = "https://api.zsxq.com/v1.8/topics/{}/comments?count=30&sort=asc".format(topic_id)
        if begin_time:
            params = {"begin_time": begin_time}
        headers = self.headers.copy()
        headers["x-version"] = "1.8.8"
        headers["Accept"] = "*/*"
        headers["Connection"] = "keep-alive" 
        headers["Origin"] = "https://wx.zsxq.com"
        headers["authorization"] = self.token.encode("utf-8")
        headers["x-request-id"] = "bf192599-f8b6-c839-c629-dea24c41{}".format(random.randint(1000,1999))
        headers["Referer"] = "https://wx.zsxq.com/dweb/"
        headers["Accept-Encoding"] = "gzip, deflate, br"
        headers["Accept-Language"] = "zh-CN,zh;q=0.9,en;q=0.8"
        # response = requests.get(url, headers=headers, proxies={"https": "127.0.0.1:8081"})
        response = requests.get(url, headers=headers, params=params)
        result_json = response.json() 
        if not result_json["succeeded"]:
            raise LoginTimeOut("请重新登录")

        return result_json["resp_data"]["comments"]

    def download_file(self, topic):
        """下载帖子中的文件"""
        images = topic.get("talk", {}).get("images", {})
        base_path = "{}/image".format(self.base_data_path)
        if not images:
            return
        for image in images:
            try:
                image_file = "{}/{}".format(base_path, image["thumbnail"].split(".")[-1])
                resp = requests.get("thumbnail")
                if resp.status_code == 200:
                    with open(image_file, "wb") as fd:
                        fd.write(resp.content)
            except Exception as errro:
                continue


        


        
    def cache_token(self):
        """cache cookie"""
        if not self.token:
            return
        with open("token.txt", "wb") as fd:
            fd.write(json.dumps({"token": self.token}))

    def get_cache_token(self):
        """cache cookie"""

        if not os.path.exists("token.txt"):
            return

        with open("token.txt", "rb") as fd:
            self.token = json.loads(fd.read())["token"]

    def cache_params(self):
        """cache cookie"""

        file_path = "data/full_params.json"

        data = {
            "pre_end_time": self.pre_end_time,
            "stop_spider_time": self.stop_spider_time,
            "group_id": self.group_id
        }

        with open(file_path, "wb") as fd:
            fd.write(json.dumps(data))

    def get_cache_params(self):
        """cache cookie"""

        file_path = "data/full_params.json"
        if not os.path.exists(file_path):
            return {}

        with open(file_path, "rb") as fd:
            return json.loads(fd.read())

    def cache_topics(self):
        """cache topics

        按月份目录存储，文件名使用topic开始时间到结束时间
        """
        if not self.topics:
            return

        directory = "{}/{}".format(self.base_data_path, arrow.now().format("YYYY-MM"))
        if os.path.exists(directory):
            os.makedirs(directory)

        begin_time = arrow.get(self.topics[0]["crate_time"]).format("YYYYMMMDDHHmm")
        end_time = arrow.get(self.topics[-1]["crate_time"]).format("YYYYMMMDDHHmm")
        file_path = "{}/topic/{}_{}.txt".format(directory, begin_time, end_time)
        if os.path.exists(file_path):
            file_path = "{}/topic/{}_{}_{}.txt".format(
                directory,
                begin_time,
                end_time,
                arrow.now().timestamp
            )
        
        with open(file_path, "wb") as fd:
            fd.write(json.dumps(self.topics))
            self.topics = []

        self.cache_comments(begin_time, end_time)

    def cache_comments(self, begin_time, end_time):
        """cache topics"""
        if not self.comments:
            return

        directory = "{}/{}".format(self.base_data_path, arrow.now().format("YYYY-MM"))

        file_path = "{}/comment/{}_{}.txt".format(directory, begin_time, end_time)
        if os.path.exists(file_path):
            file_path = "{}/comment/{}_{}_{}.txt".format(
                directory,
                begin_time,
                end_time,
                arrow.now().timestamp
            )

        with open(file_path, "wb") as fd:
            fd.write(json.dumps(self.comments))
            self.comments= []

    def check_continue_spider(self, topic):
        """检测是否继续抓取
        """
        pass
        


class FullSpider(BaseSpider):
    """全部抓取
    """
    def check_continue_spider(self, topic):
        """检测是否继续抓取
        """
        # 到达预定的爬取时间，中断爬取
        if arrow.get(topic["create_time"]) < self.stop_spider_time:
            raise StopSpiderError("停止抓取")

class DailySpider(BaseSpider):
    """日常抓取
    """
    def begin_spider(self):
        """begin spider
        """
        if not group_id:
            raise NotGroupId("缺少group_id")
        while 1:
            topic_list = self.get_topic_list(self.group_id)
            if not topic_list:
                # 休眠30分钟
                time.sleep(60*30)
            # 遍历检查topic_list，获取增量topic
            new_topic_list = self.get_increment_topic_list(topic_list)
            if not topic_list:
                # 休眠30分钟
                time.sleep(60*30)

            self.topics.extend(new_topic_list)

            for topic in topi_list:
                if topic["comments_count"] > len(topic["show_comments"]):
                    self.comments.extend(self.get_comment_list(topic))
                else:
                    self.comments.extend(topic["show_comments"])

            # 每1000条保存一次，防止数据过多崩溃
            if len(self.topics) % 1000 == 0:
                self.cache_topics()
            
            self.pre_spider_time = new_topic_list[-1]["create_time"]
            
            time.sleep(2)


    def cache_params(self):
        """cache cookie"""

        file_path = "data/daily_params.json"

        data = {
            "pre_end_time": self.pre_end_time,
            "group_id": self.group_id
        }

        with open(file_path, "wb") as fd:
            fd.write(json.dumps(data))

    def get_cache_params(self):
        """cache cookie"""

        file_path = "data/daily_params.json"
        if not os.path.exists(file_path):
            return {}

        with open(file_path, "rb") as fd:
            return json.loads(fd.read())

    def get_increment_topic_list(self, topic_list):
        """获取增量topic
        """
        new_topic_list = []
        for topic in topi_list:
            if arrow.get(topic["create_time"]) > self.pre_end_time::
                new_topic_list.append(topic)
        return new_topic_list
