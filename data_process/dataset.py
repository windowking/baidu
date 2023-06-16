#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: dataset.py
Desc: 数据集处理文件，按函数先后顺序调用
Author:gaoyang
"""
import random


def map(input_file_path, label_map_path, output_file_path):
    """
    将原始数据集中的标签换成标签映射表中清洗过后的新标签，参照lebel_map.txt文件
    Args: 
        input_file_path: 原始数据集的输入文件路径
        labeli_map_path: 标签映射表的文件路径
        output_file_path: 输出文件路径
    """
    #遍历原始数据集文件，将每个标签和对应的url字符串存入字典
    tag_url_dict = {}
    with open(input_file_path, 'r') as f1:
        for line in f1:
            tag, urls = line.strip().split('\t')
            tag_url_dict[tag] = urls

    # 遍历标签映射表，输出清洗过后的标签和对应的url字符串
    with open(label_map_path, 'r') as f2, open(output_file_path, 'w') as f3:
        for line in f2:
            old_tag, new_tag = line.strip().split('\t')
            if old_tag in tag_url_dict:
                f3.write('{}\t{}\n'.format(new_tag, tag_url_dict[old_tag]))


def same_tag_merged(input_file_path, output_file_path):
    """
    标签映射表中可能存在清洗过后的标签重复的情况，将重复标签的urls合并
    Args:
        input_file_path: 输入文件路径
        output_file_path: 标签合并后的输出文件路径
    """
    with open(input_file_path, "r") as f1, open(output_file_path, "w") as f2:
        tag_urls_dict = {}

        #合并相同标签的url字符串
        for line in f1:
            tag, urls = line.strip().split("\t")
            if tag in tag_urls_dict:
                tag_urls_dict[tag] = tag_urls_dict[tag] + "-" + urls
            else:
                tag_urls_dict[tag] = urls

        for part in tag_urls_dict.items():
            f2.write(part[0] + "\t" + part[1] + "\n")


def get_same_urls(input_file_path, same_urls_list):
    """
    收集输入文件中所有的重复urls
    Args:
        input_file_path: 输入文件路径
        same_urls_list: 输入文件中的重复的urls文件路径
    """
    with open(input_file_path, "r") as f1, open(same_urls_list, "w") as f2:
        #same_sign_set: 重复的的sign集合
        same_sign_set = set()
        sign_set = set()

        for line in f1:
            tag, urls = line.strip().split("\t")
            urls_list = urls.split("-")

            for url in urls_list:
                if not url.split('&', 1)[0].split('u=', 1)[1] in sign_set:
                    sign_set.add(url.split('&', 1)[0].split('u=', 1)[1])
                else:
                    same_sign_set.add(url.split('&', 1)[0].split('u=', 1)[1])
                        
        f2.write("\n".join(same_sign_set))


def delete_same_urls(input_file_path, output_file_path, same_urls_list):
    """
     删除数据集文件中的重复urls
     Args:
        input_file_path: 输入文件路径
        output_file_path: 输出文件路径
        same_urls_list: 输入文件中的重复的urls文件路径
    """
    with open(input_file_path, "r") as f1, open(same_urls_list, "r") as f2, open(output_file_path, "w") as f3:
        sign_set = set()
        #将重复的sign存入集合
        for line in f2:
            same_sign = line.strip()
            sign_set.add(same_sign)

        #删除重复的sign
        for line in f1:
            tmp_list = set()
            tag, urls = line.strip().split("\t")
            urls_list = urls.split("-")
            for url in urls_list:
                if not url.split('&', 1)[0].split('u=', 1)[1] in sign_set:
                    tmp_list.add(url)
            f3.write(tag + "\t" + "-".join(tmp_list) + "\n")


def delete_multiple_labels_urls(input_file_path, multiple_labels_urls_file_path, output_file_path):
    """
    删除对应多个标签的url
    Args: 
        input_file_path: 输入文件路径
        multiple_labels_urls_file_path: mr任务输出的对应多个标签的urls列表文件路径
        output_file_path: 输出文件路径
    """
    with open(input_file_path) as f1, open(multiple_labels_urls_file_path, "r") as f2, \
        open(output_file_path, "w") as f3:

        #将对应的多个标签的sign存入集合中
        sign_list = set()
        for line in f2:
            sign = line.strip()
            sign_list.add(sign)

        #删除对应多个标签的sign
        for line in f1:
            url_list = set()
            tag, urls = line.strip().split("\t")
            url_strs = urls.split("-")   
            for url_str in url_strs:
                if not url_str.split('&', 1)[0].split('u=', 1)[1] in sign_list:
                    url_list.add(url_str)
            f3.write(tag + "\t" + "-".join(url_list) + "\n")


def filter_urls_empty(input_file_path, output_file_path):
    """
    将urls字符串为empty的标签过滤
    Args: 
        input_file_path: 输入文件路径
        output_file_path: 输出文件路径
    """
    with open(input_file_path, "r") as f1, open(output_file_path, "w") as f2:
        for line in f1:
            parts = line.strip().split("\t")
            if len(parts) == 1:
                continue
            else:
                f2.write(line)


def similarity_merged(input_file_path, output_file_path, similarity_dict_path):
    """
    根据相似词合并文件将相似词的urls合并
    Args:
        input_file_path: 输入文件路径
        output_file_path: 输出文件路径
        similarity_dict_path: 相似词合并文件路径
    """
    #将输入文件的标签和urls存入字典中
    tag_to_url = {}
    with open(input_file_path, 'r') as f1:
        for line in f1:
            tag, url = line.strip().split('\t')
            tag_to_url[tag] = url

    with open(similarity_dict_path, 'r') as f2, open(output_file_path, 'w') as f3:
        for line in f2:
            parts = line.strip().split('\t')
            if len(parts) == 2:
                tag, similar_words_str = parts[0], parts[1]
                similar_words = similar_words_str.split('*')
                new_str = ''

                #相似词合并后的urls由原来标签的urls和合并进的相似词的urls组成
                if tag in tag_to_url:
                    new_str = new_str + tag_to_url[tag]
                for word in similar_words:
                    if word in tag_to_url:
                        new_str += "-" + tag_to_url[word] 
                new_str = new_str.strip()
                if len(new_str) == 0:
                    continue
            else:
                tag = parts[0]
                new_str = ''
                if tag in tag_to_url:
                    new_str = new_str + tag_to_url[tag] 
                new_str = new_str.strip()
                if len(new_str) == 0:
                    continue
            f3.write(tag + '\t' + new_str + '\n')


def collect_len100_urls(input_file_path, output_file_path):
    """
    保留urls长度超过100的标签，若urls长度超过100则随机从中保留100个
    Args: 
        input_file_path: 输入文件路径
        output_file_path: 输出文件路径
    """
    with open(input_file_path, 'r') as f1,\
     open(output_file_path, 'w') as f2:
        for line in f1:
            tag, urls = line.strip().split("\t")
            urls_list = urls.split("-")
            if len(urls_list) < 100:
                continue
            elif len(urls_list) == 100:
                f2.write(tag + "\t" + urls + "\n")
            else:
                new_urls = random.sample(urls_list, 100)
                f2.write(tag + "\t" + "-".join(new_urls) + "\n")


