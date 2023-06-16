#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: download.py
Desc: 多进程下载数据集
Author: gaoyang
"""
import os
import requests
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime


def download_image(image_url, file_path):
    """
    下载给定url的图片(最大下载次数3次)
    Args:
        image_url: 图片url
        file_path: 图片存放路径
    """
    max_retries = 3
    for retry_count in range(max_retries):
        try:
            response = requests.get(image_url, stream=True)
            if response.status_code == 200:
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                        
                return True
        except Exception as e:
            print(f'Download {image_url} failed, retrying ({retry_count+1}/{max_retries}):', e)
    print(f'Download {image_url} failed after {max_retries} retries, skipping.')
    return False


def download_images(image_list):
    """
    多进程执行的下载图片函数
    Args: 
        image_list: 数据集文本文件中的一行数据
    """
    label, url = image_list.split('\t')
    label_dir = 'download'
    
    os.makedirs(label_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    
    #时间戳加标签名的方式命名图片存放路径
    file_path = os.path.join(label_dir, "{}*{}.jpg".format(timestamp, label))

    download_image(url, file_path)
    
    
if __name__ == '__main__':
    start_time = time.time()
    with open("", "r") as f:
        image_lists = f.readlines()
    #调用进程池启动多进程执行
    with ProcessPoolExecutor(max_workers=10) as executor:
        futures = []
        for image_list in image_lists:
            futures.append(executor.submit(download_images, image_list))
        for future in as_completed(futures):
            future.result()
            futures.remove(future)
    end_time = time.time()
    print(f'Total time: {end_time - start_time:.2f} seconds')