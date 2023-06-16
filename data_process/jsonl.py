#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: jsonl.py
Desc: 生成jsonl格式的数据集文本文件
Author: gaoyang
"""
import os
import json


def get_image_prefixes(folder_path):
    """
    获得对应标签文件夹下的所有图片id列表
    Args:
        folder_path: 图片标签文件夹路径
    Return:
        image_prefixes: 标签文件夹下的所有图片id列表
    """
    image_prefixes = []
    for filename in os.listdir(folder_path):
        if filename.lower().endswith('.jpg') or filename.lower().endswith('.jpeg') or filename.lower().endswith('.png'):
            prefix = os.path.splitext(filename)[0]
            image_prefixes.append(prefix)
    return image_prefixes


def generate_json(folder_path, jsonl_file):
    """
    生成jsonl格式的数据集文本文件
    Args:
        folder_path: 包含各个标签文件夹的数据集文件夹路径
        jsonl_file: jsonl格式的数据集文本文件路径
    """
    text_id = 1
    for subfolder in os.listdir(folder_path):
        subfolder_path = os.path.join(folder_path, subfolder)
        if os.path.isdir(subfolder_path):
            #image_prefixes: 该标签文件夹下的所有图片id列表
            image_prefixes = get_image_prefixes(subfolder_path)
            json_data = {
                "text_id": text_id,
                "text": subfolder,
                "image_ids": image_prefixes
            }
            text_id += 1
            jsonl_file.write(json.dumps(json_data, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    folder_path = ''
    jsonl_path = ''
    with open(jsonl_path, 'w') as jsonl_file:
        generate_json(folder_path, jsonl_file)