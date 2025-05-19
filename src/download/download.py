# nohup /home/tangxinyu/anaconda3/envs/ictl/bin/python src/download/download.py >> logs/download.log 2>&1 &

import requests
import bs4
import os
import datetime
import time
import logging

class PeopleDailyCrawler:
    def __init__(self):
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
        }
        # 设置日志
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def _fetch_url(self, url):
        try:
            r = requests.get(url, headers=self.headers, timeout=30)
            r.raise_for_status()
            r.encoding = r.apparent_encoding
            return r.text
        except requests.exceptions.RequestException as e:
            self.logger.error(f"获取URL失败: {url}, 错误: {str(e)}")
            return None

    def _get_page_list(self, year, month, day):
        url = f'http://paper.people.com.cn/rmrb/html/{year}-{month}/{day}/nbs.D110000renmrb_01.htm'
        html = self._fetch_url(url)
        if not html:
            self.logger.warning(f"无法获取 {year}-{month}-{day} 的页面列表")
            return []
            
        try:
            bsobj = bs4.BeautifulSoup(html, 'html.parser')
            temp = bsobj.find('div', attrs={'id': 'pageList'})
            if temp:
                pageList = temp.ul.find_all('div', attrs={'class': 'right_title-name'})
            else:
                pageList = bsobj.find('div', attrs={'class': 'swiper-container'}).find_all('div', attrs={'class': 'swiper-slide'})
            
            linkList = []
            for page in pageList:
                link = page.a["href"]
                url = f'http://paper.people.com.cn/rmrb/html/{year}-{month}/{day}/{link}'
                linkList.append(url)
            return linkList
        except Exception as e:
            self.logger.error(f"解析页面列表失败: {year}-{month}-{day}, 错误: {str(e)}")
            return []

    def _get_title_list(self, year, month, day, pageUrl):
        html = self._fetch_url(pageUrl)
        if not html:
            self.logger.warning(f"无法获取标题列表: {pageUrl}")
            return []
            
        try:
            bsobj = bs4.BeautifulSoup(html, 'html.parser')
            temp = bsobj.find('div', attrs={'id': 'titleList'})
            if temp:
                titleList = temp.ul.find_all('li')
            else:
                titleList = bsobj.find('ul', attrs={'class': 'news-list'}).find_all('li')
            
            linkList = []
            for title in titleList:
                tempList = title.find_all('a')
                for temp in tempList:
                    link = temp["href"]
                    if 'nw.D110000renmrb' in link:
                        url = f'http://paper.people.com.cn/rmrb/html/{year}-{month}/{day}/{link}'
                        linkList.append(url)
            return linkList
        except Exception as e:
            self.logger.error(f"解析标题列表失败: {pageUrl}, 错误: {str(e)}")
            return []

    def _get_content(self, html):
        try:
            bsobj = bs4.BeautifulSoup(html, 'html.parser')
            title = bsobj.h3.text + '\n' + bsobj.h1.text + '\n' + bsobj.h2.text + '\n'
            
            pList = bsobj.find('div', attrs={'id': 'ozoom'}).find_all('p')
            content = ''
            for p in pList:
                content += p.text + '\n'
            
            return title + content
        except Exception as e:
            self.logger.error(f"解析文章内容失败, 错误: {str(e)}")
            return "获取内容失败"

    def _save_file(self, content, path, filename):
        try:
            if not os.path.exists(path):
                os.makedirs(path)
            
            with open(os.path.join(path, filename), 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        except Exception as e:
            self.logger.error(f"保存文件失败: {path}/{filename}, 错误: {str(e)}")
            return False

    def _download_day(self, year, month, day, destdir):
        pageList = self._get_page_list(year, month, day)
        if not pageList:
            self.logger.warning(f"没有找到 {year}-{month}-{day} 的任何页面")
            return 0
            
        article_count = 0
        for page in pageList:
            titleList = self._get_title_list(year, month, day, page)
            for url in titleList:
                try:
                    html = self._fetch_url(url)
                    if not html:
                        continue
                        
                    content = self._get_content(html)
                    
                    temp = url.split('_')[2].split('.')[0].split('-')
                    pageNo = temp[1]
                    titleNo = temp[0] if int(temp[0]) >= 10 else '0' + temp[0]
                    path = os.path.join(destdir, f'{year}{month}{day}')
                    fileName = f'{year}{month}{day}-{pageNo}-{titleNo}.txt'
                    
                    if self._save_file(content, path, fileName):
                        article_count += 1
                except Exception as e:
                    self.logger.error(f"处理文章失败: {url}, 错误: {str(e)}")
                    continue
                    
        return article_count

    @staticmethod
    def _gen_dates(b_date, days):
        day = datetime.timedelta(days=1)
        for i in range(days):
            yield b_date + day * i

    def download(self, begin_date, end_date, output_dir='./output'):
        """
        下载人民日报指定日期范围内的文章
        
        参数:
            begin_date (str): 开始日期，格式如'20220706'
            end_date (str): 结束日期，格式如'20220708'
            output_dir (str): 输出目录，默认为'./output'
        """
        try:
            start = datetime.datetime.strptime(begin_date, "%Y%m%d")
            end = datetime.datetime.strptime(end_date, "%Y%m%d")
        except ValueError as e:
            self.logger.error(f"日期格式错误: {str(e)}")
            print("日期格式错误，请使用'YYYYMMDD'格式")
            return

        print('---人民日报文章爬取系统---')
        total_articles = 0
        failed_days = []
        
        for d in self._gen_dates(start, (end-start).days + 1):
            year = str(d.year)
            month = str(d.month) if d.month >= 10 else '0' + str(d.month)
            day = str(d.day) if d.day >= 10 else '0' + str(d.day)
            
            print(f'正在下载 {year}/{month}/{day} 的文章...')
            articles = self._download_day(year, month, day, output_dir)
            
            if articles > 0:
                print(f'已下载 {year}/{month}/{day} 的 {articles} 篇文章')
                total_articles += articles
            else:
                print(f'警告: {year}/{month}/{day} 没有成功下载任何文章')
                failed_days.append(f'{year}/{month}/{day}')
        
        print('---文章爬取完成---')
        print(f'共下载 {total_articles} 篇文章')
        if failed_days:
            print(f'以下日期没有成功下载文章: {", ".join(failed_days)}')
        print(f'所有文章已保存至: {os.path.abspath(output_dir)}')


# 使用示例
if __name__ == '__main__':
    crawler = PeopleDailyCrawler()
    crawler.download('20230501', '20240430', './data/rmrb')