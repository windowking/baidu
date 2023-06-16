"""
File: get_movies_data.py
Desc: 爬取片库网站上的电影内容和迅雷下载链接
Author: gaoy
"""
import requests
from bs4 import  BeautifulSoup

movie_count = 0 #已下载电影数量


def get_html_charset(url):
    """
    获取网站内容的编码格式
    Args:
        url: 页面url链接
    Returns:
        charset: 返回的编码格式
    """
    html = requests.get(url)
    content_type = html.headers.get("Content-Type", "")
    charset = content_type.split("charset=")[-1]
    return charset


def get_html_content(url):
    """
    返回网站主页面内容
    Args:
        url: 页面url链接
    Returns:
        content: 返回的解码后的网页内容
    """
    html = requests.get(url)
    charset = get_html_charset(url)
    return html.content.decode(charset)


def get_html_data(url, file):
    """
    获得单个电影的相关内容和下载链接
    Args:
        url: 页面url链接
        file: 输出文件路径
    """
    content = get_html_content(url)
    soup = BeautifulSoup(content, 'lxml')
    movies_tag_list = soup.find_all("div", class_ = "li-bottom")

    for tag in movies_tag_list:
        movie = {}
        movie_data = tag.find("a")
        href = movie_data.get("href").strip()
        title = movie_data.get("title").strip()
        movie_url = "https://www.pkmkv.com" + href

        movie["title"] = title
        movie["origin_url"] = movie_url

        next_content = get_html_content(movie_url)
        next_soup = BeautifulSoup(next_content, "lxml")

        movie_keywords = next_soup.find("meta", attrs = {"name": "keywords"})
        movie["keywords"] = movie_keywords.get("content").strip()

        movie_content = next_soup.find("meta", attrs = {"name": "description"})
        movie["description"] = movie_content.get("content").strip()

        download_list = []
        movie_downloads = next_soup.find_all("a", class_ = "folder")

        for movie_download in movie_downloads:
            download_list.append(movie_download.get("href"))
        
        movie["download_address"] = download_list

        global movie_count
        global download_movie_num

        file.write(str(movie_count) + "-" * 20 + "\n") 
        for key, value in movie.items():
            file.write(f"{key}: {value}\n")
        file.write("\n")

        movie_count += 1

        print(f"movie {movie_count} has been successfully crawled!")


if __name__ == "__main__":
    max_page = 1000000 #爬取页面的最大值
    sort_type = "score" #(time, score, hits)
    domain_name = "https://www.pkmkv.com/" #主网站url
    output_file = "/home/gaoyang/sects/pianku_movies.txt" #输出文件路径
    max_fail_num = 0 #最大爬取失败次数

    with open(output_file, "w") as file:
        for page_index in range(max_page):
            if max_fail_num >= 10:
                print("Crawing task failed")
                break

            try:
                url = domain_name + "ms/" + "1--" + sort_type + "------" + str(page_index) + "---" + ".html"
                get_html_data(url, file)
                print(f'page {page_index} has been completed!')
                max_fail_num = 0
            except:
                print(f"Failed to crawl page {page_index}")
                max_fail_num += 1
