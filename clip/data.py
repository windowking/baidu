#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File: data.py
Desc: 数据集载入和处理
Author: gaoyang
"""


from math import ceil
import os
from pathlib import Path
import json
from PIL import Image
import base64
from io import BytesIO
from dataclasses import dataclass
import numpy as np
import torch
from torch.utils.data import Dataset, DataLoader
from torch.utils.data.distributed import DistributedSampler
from torchvision.transforms import Compose, Resize, ToTensor, Normalize, InterpolationMode
from timm.data import create_transform
import pandas as pd
import random
from cn_clip.clip import _tokenizer
from cn_clip.clip import tokenize


def _convert_to_rgb(image):
    return image.convert('RGB')


def _preprocess_text(text):
    # adapt the text to Chinese BERT vocab
    text = text.lower().replace("“", "\"").replace("”", "\"")
    return text


class ClipDataset(Dataset):
    """
    数据集加载类
    """
    def __init__(self, dataset_path, split="val", max_txt_length=64, use_augment=False, resolution=224):
        """
        初始化函数
        Args: 
            dataset_path: 数据集路径
            split: 数据集划分类型
            others: 其他参数(均使用默认值)
        """
        assert os.path.isdir(dataset_path), f"The LMDB directory {dataset_path} of {split} split does not exist!"
        self.dataset_path = dataset_path
        self.label_img_files = os.listdir(self.dataset_path)
        self.split = split

        self.dataset_len = self._get_dataset_len()
        print(self.dataset_len)
        self.max_txt_length = max_txt_length
        self.use_augment = use_augment
        self.resolution = resolution
        self.transform = self._build_transform()

        #随机读取一个数据集文件
        self.read_file = random.choice(self.label_img_files)
        self.label_img_files.remove(self.read_file)
        self.chunk = pd.read_csv(os.path.join(self.dataset_path, self.read_file), sep='\t', header=None)

    def _get_dataset_len(self):
        """
        获取数据集大小
        """
        # total_lines = 0
        # for root, dirs, files in os.walk(self.dataset_path):
        #     for file in files:
        #         if file.endswith('.tsv'):
        #             file_path = os.path.join(root, file)
        #             df = pd.read_csv(file_path, sep='\t')
        #             total_lines += len(df)
        # return total_lines

        #写死数据集文件数量，防止每次都要重新计算数据集文件数量
        if self.split == "train":
            return 2999390
        else:
            return 774742
        
    def _build_transform(self):
        if self.use_augment:
            transform = create_transform(
                input_size=self.resolution,
                scale=(0.9, 1.0),
                is_training=True,
                color_jitter=None,
                auto_augment="original",
                interpolation="bicubic",
                mean=(0.48145466, 0.4578275, 0.40821073),
                std=(0.26862954, 0.26130258, 0.27577711),
            )
            transform = Compose(transform.transforms[:-3] + [_convert_to_rgb] + transform.transforms[-3:])
        else:
            transform = Compose(
                [
                    Resize((self.resolution, self.resolution), interpolation=InterpolationMode.BICUBIC),
                    _convert_to_rgb,
                    ToTensor(),
                    Normalize((0.48145466, 0.4578275, 0.40821073), (0.26862954, 0.26130258, 0.27577711)),
                ]
            )
        return transform

    def __len__(self):
        return self.dataset_len

    def __getitem__(self, index: int):
        """
        获得随机索引的数据集元素
        Args: 
            index: 索引
        Returns:
            image: 随机索引的图片内容
            text: 随机索引的文本内容
            eos_index: '[SEP]' 标记的索引位置
        """
        #当数据集文件列表都被载入完时，重新获取文件列表
        if len(self.label_img_files) == 0:
            self.label_img_files = os.listdir(self.dataset_path)
        
        #当载入的数据集文件内的元素都被读取完时，重新载入一个新的数据集文件
        if len(self.chunk) <= 0:
            self.read_file = random.choice(self.label_img_files)
            self.label_img_files.remove(self.read_file)
            self.chunk = pd.read_csv(os.path.join(self.dataset_path, self.read_file), sep='\t', header=None)

        #从载入的数据集文件中随机读取一个元素，并将读取过的元素删除
        random_index = random.randint(0, len(self.chunk) - 1)
        random_row = self.chunk.sample(n=1, random_state=random_index)
        self.chunk.drop(random_row.index, inplace=True)

        text = random_row.iloc[0, 0]
        image_b64 = random_row.iloc[0, 1] 

        if text == None or image_b64 == None:
            return None, None, None
        
        #处理图片和标签
        image_b64_bytes = image_b64.encode('utf-8')
        image_data = base64.b64decode(image_b64_bytes)
        image = Image.open(BytesIO(image_data))
        image = self.transform(image)
        text = tokenize([_preprocess_text(text)], context_length=self.max_txt_length)[0]
        eos_index = text.numpy().tolist().index(_tokenizer.vocab['[SEP]'])

        return image, text, eos_index


def pad_dataset(dataset, global_batch_size):
    # edit dataset.__len__() of the dataset
    dataset.dataset_len = ceil(dataset.dataset_len / global_batch_size) * global_batch_size
    dataset.global_batch_size = global_batch_size


def fetch_resolution(vision_model):
    # fetch the resolution from the vision model config
    vision_model_config_file = Path(__file__).parent.parent / f"clip/model_configs/{vision_model.replace('/', '-')}.json"
    with open(vision_model_config_file, 'r') as fv:
        model_info = json.load(fv)
    return model_info["image_resolution"]


@dataclass
class DataInfo:
    dataloader: DataLoader
    sampler: DistributedSampler
    dataset: ClipDataset
    epoch_id: int


def get_dataset(args, is_train, max_txt_length=64, epoch_id=0):
    """
    获取用于训练或验证的数据集
    Args:
        args: 命令行参数
        is_train: 是否为训练集
        max_txt_length: 最大文本长度
        epoch_id: 训练的epoch_id
    Returns:
        DataInfo: 包含训练或验证的数据加载器、采样器、数据集以及当前的训练轮数等信息
    """
    if is_train:
        db_path = args.train_data
    else:
        db_path = args.val_data
    assert db_path is not None

    dataset = ClipDataset(
        db_path, 
        split="train" if is_train else "val",
        max_txt_length=max_txt_length,
        use_augment=args.use_augment if is_train else False,
        resolution=fetch_resolution(args.vision_model),
    ) 

    # pad the dataset splits using the beginning samples in the LMDB files
    # to make the number of samples enough for a full final global batch
    batch_size = args.batch_size if is_train else args.valid_batch_size
    global_batch_size = batch_size * torch.distributed.get_world_size()
    pad_dataset(dataset, global_batch_size)

    num_samples = dataset.dataset_len
    # Update in 22.12.11: We have changed the **validation** dataset sampler during finetuning
    # from sequential to shuffled (in a determistic order between experiments and epochs). 
    # This is to avoid there being one text matching multiple images (or vice versa) in a local batch
    # which will affect the correctness of computing the validation in-batch accuracy.
    sampler = DistributedSampler(dataset, shuffle=True, seed=args.seed)
    sampler.set_epoch(epoch_id if is_train else 0)

    dataloader = DataLoader(
        dataset,
        batch_size=batch_size,
        pin_memory=False,
        num_workers=args.num_workers if is_train else args.valid_num_workers,
        sampler=sampler,
    )

    dataloader.num_samples = num_samples
    assert num_samples % dataset.global_batch_size == 0
    dataloader.num_batches = num_samples // dataset.global_batch_size

    return DataInfo(dataloader, sampler, dataset, epoch_id)


def get_data(args, epoch_id=0, max_txt_length=64):
    data = {}

    if args.train_data:
        data["train"] = get_dataset(
            args, 
            is_train=True,  
            max_txt_length=max_txt_length, 
            epoch_id=epoch_id)

    if args.val_data:
        data["val"] = get_dataset(
            args, 
            is_train=False, 
            max_txt_length=max_txt_length, 
            epoch_id=epoch_id)

    return data
