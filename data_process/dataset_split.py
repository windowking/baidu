#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: dataset_split.py
Desc: 数据集打乱分片
Author: gaoyang
"""
import os
import random


def randomized(input_file, output_file):
    """
    随机打乱数据集
    Args: 
        input_file: 输入文件路径
        output_file: 输出文件路径
    """
    with open(input_file, "r") as f1:
        lines = f1.readlines()
    random.shuffle(lines)
    
    with open(output_file, "w") as f2:
        f2.writelines(lines)


def split_file(input_file, output_dir, lines_per_file=600000):
    """
    按照行数分片
    Args:
        input_file: 输入文件路径
        output_dir: 输出文件夹
        lines_per_file: 分片的行数大小
    """
    with open(input_file, 'r') as f:
       
       #初始化文件序号和行数序号
        line_count = 0
        file_count = 1
        
        for line in f:
            #满足一个分片行数时创建一个新文件
            if line_count % lines_per_file == 0:
                if line_count > 0:
                    out_file.close()
                out_file_name = os.path.join(output_dir, f'output_{file_count}.txt')
                out_file = open(out_file_name, 'w')
                file_count += 1
          
            out_file.write(line)
            line_count += 1
       
        out_file.close()


if __name__ == "__main__":
    randomized("", "")
    split_file("", "", lines_per_file=600000)
