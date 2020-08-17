import requests
from bs4 import BeautifulSoup
import random
import bs4
import functools

my_headers = [
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.25 Safari/537.36 Core/1.70.3741.400 QQBrowser/10.5.3863.400"
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14",
    "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)",
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',
    'Opera/9.25 (Windows NT 5.1; U; en)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
    'Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)',
    'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12',
    'Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9',
    "Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.7 (KHTML, like Gecko) Ubuntu/11.04 Chromium/16.0.912.77 Chrome/16.0.912.77 Safari/535.7",
    "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0 "
]
headers = {'User-Agent': random.choice(my_headers)}


def fans_num(url_offset):  # 获取某人的粉丝数
    try:
        resp = requests.get("https://movie.douban.com" + url_offset, headers=headers)
        resp.encoding = "utf-8"
        s = BeautifulSoup(resp.text, "html.parser")
        return str(s.find('div', id="fans", class_="mod").h2).split('\n')[1].split('（')[1].split('）')[0]
    except:
        return 0


def ave_all_movies(url_offset):  # 获取某人所有的作品的平均评分
    score = []
    r = requests.get("https://movie.douban.com" + url_offset, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")
    best_url = soup.find('div', id="best_movies").find('div', class_='hd').h2.span.a.attrs['href'].split("?")[0]
    r = requests.get(best_url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")

    page_url = [best_url]
    try:
        for e in soup.find('div', class_="paginator").find_all('a')[:-1]:
            page_url.append(e.attrs['href'])
    except:
        pass
    for e in page_url:
        new_url = best_url + e
        new_r = requests.get(new_url, headers=headers)
        soup = BeautifulSoup(new_r.text, "html.parser")
        for it in soup.find_all('div', class_="star clearfix"):
            try:
                score.append(float(it.find_all('span')[1].string))
            except:
                continue
    try:
        return "{:.1f}".format(sum(score) / len(score))
    except:
        return 0.0


url = "https://movie.douban.com/j/search_subjects?type=movie&tag=%E7%BB%8F%E5%85%B8&sort=recommend&page_limit=20&page_start="
# cover_url = []
# 电影名 豆瓣评分 电影类型  制片国家/地区  语言  上映日期  时长  导演名  粉丝数  所有作品评分平均数  主演  粉丝数  所有作品评分平均数
with open("movies03.txt", 'w') as f:
    for i in range(453, 500):  # 0 页面开始位置， 结束位置1000，如果打印出 《"出错位置：" 一个数字》，一般是ip被封，换个网络之后将之前已经写入的文件movies——3.txt
        try:  # 中的内容拷贝保存到#一个保存最终所有数据的文件#中，再将起始位置 改为 打印出来的出错位置，重新运行，循环往复
            movies_one_page = []  # ，由于要访问多级页面，爬虫速度较慢
            r = requests.get(url + str(i), headers=headers)
            r.encoding = r.apparent_encoding

            item = eval(r.text.replace("false", '"false"').replace("true", '"true"'))["subjects"][0]  # 每页 取第一个有效值
            line = list()  # 文件中每一行内容
            line.append(item['title'])  # 电影名
            line.append(item['rate'])  # 豆瓣评分
            # cover_url.append(item['cover'])  # 封面图片url

            resp = requests.get(item['url'].replace("\\", ""), headers=headers)  # 访问二级页面
            soup = BeautifulSoup(resp.text, "html.parser")
            movie_type = functools.reduce(lambda x, y: x + "/" + y,
                                          [e.string for e in soup.find_all('span', property="v:genre")])
            line.append(movie_type)  # 电影类型

            area_lang = []
            for e in list(filter(lambda x: False if str(x) == '\n' or str(x) == '<br/>' or str(x) == " / " or
                                                    str(x) == ' ' else True, soup.find('div', id="info").contents)):
                if type(e) is bs4.element.NavigableString:
                    area_lang.append(e.string.replace(" ", ''))
            line.append(area_lang[0])  # 制片国家/地区
            line.append(area_lang[1])  # 语言

            line.append(str(soup.find('span', property="v:initialReleaseDate").string).split("(")[0])  # 上映日期

            line.append(str(soup.find('span', property="v:runtime").string).split("分")[0])  # 时长

            director = soup.find('span', class_="attrs").a
            name = director.string
            line.append(name)  # 导演名
            href = director.attrs['href']
            fans = fans_num(href)
            line.append(str(fans))  # 粉丝数
            ave_score = ave_all_movies(href)
            line.append(str(ave_score))  # 所有作品评分平均数

            try:
                actor = soup.find('span', class_='actor').find('span', class_='attrs').contents
            except:
                pass
            try:
                for elem in actor[0: 3]:  # 可调节主演数目
                    if type(elem) is bs4.element.Tag:
                        offset_url = str(elem.attrs['href'])
                        fans = fans_num(offset_url)
                        line.append(str(elem.string))  # 主演
                        line.append(fans)  # 粉丝数
                        line.append(str(ave_all_movies(offset_url)))  # 所有作品评分平均数
            except:
                pass
            # print(line)
            res = ("\t".join(line))
            f.write(res + "\n")
            print(res)
        except:
            f.close()
            print("出错位置：" + str(i))
            break
