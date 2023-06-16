#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: tsv.py
Desc:图片格式转换，将下载的数据集转化成label + "\t" + image_base64的文本格式
Author: gaoyang
"""
import os
import base64
from PIL import Image
from io import BytesIO
import csv
import multiprocessing


def process_image_file(file_path):
    """
    处理图片文件，返回标签名和图片的base64编码
    Args: 
        file_path: 图片文件路径
    Returns:
        label: 标签名
        base64_image: 图片的base64编码
    """
    try:
        filename, label = os.path.splitext(os.path.basename(file_path))[0].split("*")
        #将图片变成base64编码
        with Image.open(file_path) as img:
            img_buffer = BytesIO()
            img.save(img_buffer, format=img.format)
            byte_data = img_buffer.getvalue()
            base64_str = base64.b64encode(byte_data).decode("utf-8")

        return label, base64_str
    except Exception as e:
        print("Error processing file")
        return None


if __name__ == '__main__':
   
    num_processes = 10
    pool = multiprocessing.Pool(processes=num_processes)

    with open("/home/gaoyang/test_results/dataset.tsv", "w") as f:
        writer = csv.writer(f, delimiter='\t')

        for subdir, dirs, files in os.walk(""):
            file_paths = [os.path.join(subdir, f) for f in files if f.endswith(".jpg")]

            #使用多进程处理所有图片
            for result in pool.imap_unordered(process_image_file, file_paths):
                if result is not None:
                    writer.writerow(result)