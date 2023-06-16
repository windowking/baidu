"""
File: reduce_test.py
Desc: 收集每个标签的随机500个不重复的url，若不足500，则全部输出
Author: gaoyang
"""
import sys
import random
reload(sys)
sys.setdefaultencoding("utf-8")


def reduce_main():
    """
    收集每个标签的随机500个不重复的url，若不足500，则全部输出
    """
    core = None
    cur_core = None
    sign_list = set()
    url_list = set()
    for line in sys.stdin:
        line = line.strip()
        try:
            core, url = line.split("\t")
            #得到url中的sign部分
            sign = url.split('&', 1)[0].split('u=', 1)[1]
        except:
            continue
        
        if cur_core == core:
            #保证每个标签的url列表不存在重复的url
            if not sign in sign_list:
                sign_list.add(sign)
                url_list.add(url)
        else:
            if cur_core: 
                #若不足500，则全部输出
                if len(url_list) < 500:
                    random_set = set(random.sample(url_list, len(url_list)))
                #若超过500，从中随机选500个输出
                else:
                    random_set = set(random.sample(url_list, 500))
                print "%s\t%s" % (cur_core, str(len(url_list)) + "\t" + "-".join(random_set))
            cur_core = core
            url_list = set()
            url_list.add(url)
            sign_list = set()
            sign_list.add(sign)
    
    if not core:
        return
    if cur_core == core:
        if len(url_list) < 500:
            random_set = set(random.sample(url_list, len(url_list)))
        else:
            random_set = set(random.sample(url_list, 500))
        print "%s\t%s" % (cur_core, str(len(url_list)) + "\t" + "-".join(random_set))


if __name__ == "__main__":
    reduce_main()