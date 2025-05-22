# 后台运行命令: nohup python ./src/download/download.py > logs/rmrb_downloader.log 2>&1 & 

import requests
import bs4
import os
import sys
import datetime
import time
import json
from tqdm import tqdm

ODQA_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ODQA_ROOT_PATH)

class RMRBDownloader:
    def __init__(self, begin_date=None, end_date=None, dest_dir="./data/PeoplesDaily"):
        """
        人民日报爬虫类初始化
        :param begin_date: 开始日期(格式如20220706)
        :param end_date: 结束日期(格式如20220706)
        :param dest_dir: 保存文件的目录
        """
        self.begin_date = begin_date
        self.end_date = end_date
        self.dest_dir = dest_dir
        self.data_dict = {}
        
        # 添加重试配置
        self.max_retries = 3
        self.retry_delay = 0  # 秒

    def _fetch_url(self, url, retries=None):
        """
        获取网页内容，支持重试机制
        :param url: 要获取的URL
        :param retries: 当前重试次数
        :return: 网页内容或None
        """
        if retries is None:
            retries = self.max_retries
            
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
        }

        try:
            r = requests.get(url, headers=headers, timeout=10)  # 添加超时设置
            
            # 检查状态码
            if r.status_code == 404:
                return None
                
            r.raise_for_status()
            r.encoding = r.apparent_encoding
            return r.text
            
        except requests.exceptions.Timeout:
            if retries > 0:
                time.sleep(self.retry_delay)
                return self._fetch_url(url, retries - 1)
            else:
                return None
                
        except requests.exceptions.HTTPError as e:
            if retries > 0 and e.response.status_code >= 500:  # 只有服务器错误才重试
                time.sleep(self.retry_delay)
                return self._fetch_url(url, retries - 1)
            else:
                return None
                
        except requests.exceptions.ConnectionError:
            if retries > 0:
                time.sleep(self.retry_delay)
                return self._fetch_url(url, retries - 1)
            else:
                return None
                
        except requests.exceptions.RequestException:
            return None

    def _get_page_list(self, year, month, day):
        """
        获取版面列表
        :return: 版面URL列表
        """
        url = f'http://paper.people.com.cn/rmrb/html/{year}-{month}/{day}/nbs.D110000renmrb_01.htm'
        print(f"🔍 正在获取版面列表: {year}-{month}-{day}")
        
        html = self._fetch_url(url)
        if html is None:
            print(f"❌ 日期 {year}-{month}-{day} 的版面列表获取失败，可能该日报纸不存在")
            return []
            
        try:
            bsobj = bs4.BeautifulSoup(html, 'html.parser')
            temp = bsobj.find('div', attrs={'id': 'pageList'})
            if temp:
                pageList = temp.ul.find_all('div', attrs={'class': 'right_title-name'})
            else:
                pageList = bsobj.find('div', attrs={
                                    'class': 'swiper-container'}).find_all('div', attrs={'class': 'swiper-slide'})
                                    
            if not pageList:
                print(f"⚠️ 日期 {year}-{month}-{day} 的版面列表为空，可能页面结构已改变")
                return []
                
            linkList = []
            for page in pageList:
                try:
                    link = page.a["href"]
                    url = f'http://paper.people.com.cn/rmrb/html/{year}-{month}/{day}/{link}'
                    linkList.append(url)
                except (AttributeError, KeyError):
                    continue
            
            print(f"✅ 日期 {year}-{month}-{day} 成功获取 {len(linkList)} 个版面")
            return linkList
            
        except Exception as e:
            print(f"❌ 解析版面列表异常: {year}-{month}-{day} - {str(e)}")
            return []

    def _get_title_list(self, year, month, day, page_url):
        """
        获取文章列表
        :return: 文章URL列表
        """
        print(f"📋 正在获取版面文章: {page_url.split('/')[-1]}")
        
        html = self._fetch_url(page_url)
        if html is None:
            print(f"❌ 版面文章列表获取失败: {page_url}")
            return []
            
        try:
            bsobj = bs4.BeautifulSoup(html, 'html.parser')
            temp = bsobj.find('div', attrs={'id': 'titleList'})
            if temp:
                titleList = temp.ul.find_all('li')
            else:
                titleList = bsobj.find(
                    'ul', attrs={'class': 'news-list'}).find_all('li')
                    
            if not titleList:
                print(f"⚠️ 版面文章列表为空: {page_url}")
                return []
                
            linkList = []
            for title in titleList:
                try:
                    tempList = title.find_all('a')
                    for temp in tempList:
                        link = temp["href"]
                        if 'nw.D110000renmrb' in link:
                            url = f'http://paper.people.com.cn/rmrb/html/{year}-{month}/{day}/{link}'
                            linkList.append(url)
                except (AttributeError, KeyError):
                    continue
            
            print(f"✅ 版面 {page_url.split('/')[-1]} 成功获取 {len(linkList)} 篇文章链接")
            return linkList
            
        except Exception as e:
            print(f"❌ 解析文章列表异常: {page_url} - {str(e)}")
            return []

    def _get_content(self, html, url):
        """
        获取文章内容
        :return: 文章内容字典或None
        """
        if not html:
            print(f"❌ 获取失败: {url} (无HTML内容)")
            return None
            
        print(f"📄 正在解析: {url}")    
        try:
            bsobj = bs4.BeautifulSoup(html, 'html.parser')
            
            # 获取标题，处理可能缺失的情况
            title_parts = []
            for title_tag in ['h3', 'h1', 'h2']:
                try:
                    tag = bsobj.find(title_tag)
                    if tag:
                        title_parts.append(tag.text)
                except:
                    pass
                    
            if not title_parts:
                title = "无标题"
                print(f"⚠️ 警告: 未找到标题 - {url}")
            else:
                title = '\n'.join(title_parts)
                
            # 获取内容
            content_div = bsobj.find('div', attrs={'id': 'ozoom'})
            if not content_div:
                print(f"❌ 获取失败: {url} (未找到内容区域)")
                return None
                
            pList = content_div.find_all('p')
            if not pList:
                print(f"❌ 获取失败: {url} (内容为空)")
                return None
                
            content = ''
            for p in pList:
                content += p.text + '\n'
                
            content_stripped = content.strip()
            title_stripped = title.strip()
            resp = {"url": url, "title": title_stripped, "content": content_stripped}
            
            # 打印成功信息，显示标题和内容长度
            print(f"✅ 成功获取: [{title_stripped[:20]}{'...' if len(title_stripped) > 20 else ''}] ({len(content_stripped)} 字符)")
            return resp
            
        except AttributeError as e:
            print(f"❌ 解析错误: {url} - 属性错误: {str(e)}")
            return None
        except Exception as e:
            print(f"❌ 未知错误: {url} - {str(e)}")
            return None

    def _save_json_file(self, data, path, filename):
        """
        保存JSON文件
        """
        if not data:
            return False
            
        try:
            if not os.path.exists(path):
                os.makedirs(path)
                
            with open(os.path.join(path, filename), 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                
            return True
            
        except IOError:
            return False
        except Exception:
            return False

    def _download_rmrb(self, year, month, day):
        """
        下载指定日期的人民日报文章
        """
        print(f"📅 开始下载 {year}-{month}-{day} 的文章")
        
        pageList = self._get_page_list(year, month, day)
        if not pageList:
            print(f"📭 日期 {year}-{month}-{day} 没有可用版面，跳过")
            return 0
            
        article_count = 0
        downloaded_count = 0
        daily_articles = []
        
        for i, page in enumerate(pageList, 1):
            print(f"⏳ 处理第 {i}/{len(pageList)} 个版面: {page.split('/')[-1]}")
            titleList = self._get_title_list(year, month, day, page)
            article_count += len(titleList)
            
            if not titleList:
                continue
                
            for j, url in enumerate(titleList, 1):
                print(f"  ⏳ 处理第 {j}/{len(titleList)} 篇文章...")
                html = self._fetch_url(url)
                if html is None:
                    continue
                    
                content = self._get_content(html, url)
                if content is None:
                    continue
                
                daily_articles.append(content)
                downloaded_count += 1
                
                # 添加爬取延迟，避免请求过于频繁
                # time.sleep(0.5)
        
        # 将当天文章保存到数据字典
        key = f'{year}{month}'
        if key not in self.data_dict:
            self.data_dict[key] = []
        self.data_dict[key].extend(daily_articles)
        
        success_rate = 0 if article_count == 0 else (downloaded_count / article_count) * 100
        print(f"📊 日期 {year}-{month}-{day} 统计: {downloaded_count}/{article_count} 篇文章下载成功 (成功率: {success_rate:.1f}%)")
        return downloaded_count

    def _gen_dates(self, b_date, days):
        """生成日期序列"""
        day = datetime.timedelta(days=1)
        for i in range(days):
            yield b_date + day * i

    def _get_date_list(self, begin_date, end_date):
        """获取日期范围列表"""
        try:
            start = datetime.datetime.strptime(begin_date, "%Y%m%d")
            end = datetime.datetime.strptime(end_date, "%Y%m%d")
            
            if start > end:
                return []
                
            data = []
            for d in self._gen_dates(start, (end-start).days + 1):  # +1确保包含结束日期
                data.append(d)
                
            return data
            
        except ValueError:
            return []
            
    def clean_json_files(self):
        for filename in tqdm(os.listdir(self.dest_dir)):
            if filename.endswith(".json"):
                file_path = os.path.join(self.dest_dir, filename)
                with open(file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)

                # 保留不符合条件的条目
                cleaned_data = [
                    item for item in data if '本版责编' not in item['title'] and item['content'].strip() != '']

                with open(file_path, 'w', encoding='utf-8') as file:
                    json.dump(cleaned_data, file, ensure_ascii=False, indent=4)
                    
    def run(self):
        """
        运行爬虫
        :return: 文章数据字典
        """ 
        if not self.begin_date or not self.end_date:
            print("❌ 错误: 开始日期和结束日期不能为空")
            return None
        
        print(f"🚀 开始爬取人民日报文章，日期范围: {self.begin_date} 至 {self.end_date}")
        
        data = self._get_date_list(self.begin_date, self.end_date)
        if not data:
            print("❌ 错误: 日期列表为空，无法继续")
            return None
            
        print(f"📆 共有 {len(data)} 天需要处理")
        
        self.data_dict = {}
        total_articles = 0
        saved_files = 0
        success_days = 0
        empty_days = 0
        
        # 按月分组处理
        current_month = None
        
        # 使用tqdm显示进度
        for d in tqdm(data, desc="📈 爬取进度"):
            year = str(d.year)
            month = str(d.month) if d.month >= 10 else '0' + str(d.month)
            day = str(d.day) if d.day >= 10 else '0' + str(d.day)
            year_month = f"{year}{month}"
            
            # 如果月份变化，保存前一个月的数据
            if current_month and current_month != year_month and current_month in self.data_dict and self.data_dict[current_month]:
                print(f"\n{'='*50}")
                print(f"💾 保存 {current_month[:4]}-{current_month[4:]} 月数据...")
                filename = f"{current_month[:4]}-{current_month[4:]}.json"
                
                if self._save_json_file(self.data_dict[current_month], self.dest_dir, filename):
                    saved_files += 1
                    article_count = len(self.data_dict[current_month])
                    print(f"✅ 已保存: {filename} ({article_count} 篇文章)")
                    
                # 清空已保存的月份数据，减少内存占用
                self.data_dict[current_month] = []
            
            # 更新当前月份
            current_month = year_month
            
            print(f"\n{'='*50}")
            print(f"📅 开始处理 {year}-{month}-{day}")
            print(f"{'='*50}")
            
            try:
                articles = self._download_rmrb(year, month, day)
                total_articles += articles
                
                if articles > 0:
                    success_days += 1
                else:
                    empty_days += 1
                    
                print(f"✓ {year}-{month}-{day} 完成，获取 {articles} 篇文章")
                print(f"{'='*50}\n")
                
            except Exception as e:
                print(f"❌ 处理 {year}-{month}-{day} 时出错: {str(e)}")
                empty_days += 1
                print(f"{'='*50}\n")
                continue
        
        # 保存最后一个月的数据
        for key, value in self.data_dict.items():
            if not value:  # 跳过空数据
                continue
                
            year, month = key[:4], key[4:]
            filename = f'{year}-{month}.json'
            
            print(f"\n{'='*50}")
            print(f"💾 保存 {year}-{month} 月数据...")
            
            if self._save_json_file(value, self.dest_dir, filename):
                saved_files += 1
                print(f"✅ 已保存: {filename} ({len(value)} 篇文章)")
        
        print(f"\n{'='*50}")
        print(f"📊 爬取统计:")
        print(f"   📑 总文章数: {total_articles} 篇")
        print(f"   📁 保存文件: {saved_files} 个")
        print(f"   📅 成功天数: {success_days}/{len(data)} 天")
        print(f"   📭 空内容天数: {empty_days}/{len(data)} 天")
        print(f"{'='*50}")
        return self.data_dict


if __name__ == '__main__':
    print("\n" + "="*60)
    print("🗞️  人民日报文章爬虫 v1.0")
    print("="*60)
    
    # 示例用法
    dir = "./data/PeoplesDaily"
    if os.path.exists(dir):
        print("🧹 清理现有JSON文件中...")
        downloader = RMRBDownloader('20230501', '20240430')
        downloader.clean_json_files()
        print("✅ 文件清理完成！")
    else:
        print("🔍 开始爬取人民日报文章，日期范围: 20230501 至 20240430")
        downloader = RMRBDownloader('20230501', '20240430')
        # downloader = RMRBDownloader('20230501', '20230502')
        downloader.run()
        print("🧹 开始清理JSON文件...")
        downloader.clean_json_files()
        print("✅ 任务全部完成！")
    
    print("="*60)