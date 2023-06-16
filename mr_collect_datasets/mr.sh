#/bin/bash
source /home/work/.bash_profile

HADOOP_BIN=/home/work/b2b-ods/tools/hadoop-client/hadoop/bin/hadoop
CONF_BIN=/home/work/liuzhuang03/tool/conf/hadoop-site.b2bnew.xml
LOCAL_BIN_PATH=/home/work/gaoy/dataset/cp_url_list

INPUT_PATH=/user/b2b-new/yuanguanhong/b2b/b2b_commodity_detail/*   #小批量跑一个文件 part-00001.gz
OUTPUT_PATH=/user/b2b-new/liuzhuang03/gaoy/output_datasets_500

# 删除已有目录，hadoop检测到已存在的目录则直接任务失败
$HADOOP_BIN fs -conf ${CONF_BIN} -rmr ${OUTPUT_PATH}

$HADOOP_BIN streaming -conf ${CONF_BIN} \
    -cacheArchive "/user/b2b-new/huangzhibiao/tools/python2.7_bs4.zip#python" \
    -input ${INPUT_PATH} \
    -output ${OUTPUT_PATH} \
    -mapper "python/python2.7/bin/python mapper_test.py" \
    -reducer "python/python2.7/bin/python reduce_test.py" \
    -file "${LOCAL_BIN_PATH}/mapper_test.py" \
    -file "${LOCAL_BIN_PATH}/reduce_test.py" \
    -jobconf mapred.job.name="search_url_list"  \
    -jobconf mapred.job.priority=NORMAL \
    -jobconf stream.memory.limit=30000 \
    -jobconf mapred.reduce.tasks=1000 \
    -jobconf mapred.job.map.capacity=300 \
    -jobconf mapred.job.reduce.capacity=600 \
    -jobconf map.output.key.field.separator="\t" \
    -jobconf num.key.fields.for.partition=1 \
    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner