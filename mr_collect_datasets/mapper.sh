"""
File: mapper_test.py
Desc: 输出符合要求的(新标签, url)键值对
Author: gaoyang
"""
import sys
import json
import re


reload(sys)
sys.setdefaultencoding("utf-8")


#设置正则表达式匹配内容，以汉字、数字、字母、逗号、减号或括号组成的字符串
regex = re.compile(ur'^[\u4e00-\u9fa5\w,\-\(\)]+$')
tag_value = []


def is_valid_parentheses(s):
    """判断标签词是否包含完整的一对括号"""
    stack = []
    for c in s:
        if c == '(':
            stack.append(c)
        elif c == ')':
            if not stack or stack.pop() != '(':
                return False
    return not stack


def validate_string(input_str):
    """对标签词进行正则表达式匹配"""
    if re.match(regex, input_str):
        return True
    else:
        return False


def mapper_main():
    """
    输出符合要求的(标签词, url)的键值对
    """
    for line in sys.stdin:
        try:
            bsonItem = json.loads(line.strip("\n").split("\t")[1].replace(" ", "").replace("\t", "").replace("\n", ""))
            if bsonItem["@type"] != "b2b_commodity_detail" or \
               bsonItem["@site"] == "aladdin" or bsonItem["payType"] == "0":
                continue
            
            #保存商品中的主图url
            big_url_size = len(bsonItem.get("dpsImage"))
            url_list = []
            for i in range(big_url_size):
                url_list.append(bsonItem.get("dpsImage")[i].get("big_url"))
            if not url_list:
                continue
            
            #获取商品的标签词
            core_products = bsonItem.get("core_products")
            if not core_products:
                continue
            
            #获取商品的prds
            prd_list = []
            title_ner_list = bsonItem.get("title_ner")
            title_ner_list_size = len(title_ner_list)
            for i in range(title_ner_list_size):
                if title_ner_list[i][1] == "prd":
                    text = title_ner_list[i][0]
                    if "\\u" in text:
                        try:
                            text = text.encode("utf-8").decode("unicode_escape")
                        except Exception as err:
                            pass      
                    #判断该prd是否符合要求            
                    if validate_string(text) == True and is_valid_parentheses(text) == True:
                        prd_list.append(text)
            if not prd_list:
                continue
            
            #如果一个标签词内包含多个prd，则不保留此标签词
            #若一个标签词内只包含一个prd，则保留此标签词
            new_core_products = []

            for core in core_products:
                prd_count = 0
                for prd in prd_list:
                    if core.find(prd) != -1:
                        if prd_count == 0:
                            new_core_products.append(prd) 
                            prd_count = prd_count + 1
                        else:
                            new_core_products = []
                            break

            if not new_core_products:
                continue
                
            #输出符合要求的(新标签, url)键值对
            for cp in new_core_products:
                for url in url_list:
                    print("{}\t{}".format(cp, url))       
                     
        except:
            continue


if __name__ == '__main__':
    mapper_main()