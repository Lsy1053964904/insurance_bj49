#1、将数据用sqoop导入到hive表
/export/server/sqoop/bin/sqoop import \
--connect jdbc:mysql://192.168.88.163:3306/insurance \
--username root \
--password 123456 \
--table mort_10_13 \
--hive-table insurance_ods.mort_10_13 \
--hive-import \
--hive-overwrite \
--fields-terminated-by '\t' \
--delete-target-dir \
-m 1

#2、统计mysql的表的条数
mysql_log=`sqoop eval \
--connect jdbc:mysql://192.168.88.163:3306/insurance \
--username root \
--password 123456 \
--query "select count(1) from mort_10_13 "`
mysql_cnt=`echo $mysql_log | awk -F'|' {'print $4'} | awk {'print $1'}`
#3、统计hive 的表的条数
hive_log=`spark-sql -e "select count(1) from insurance_ods.mort_10_13 "`
hive_cnt=`echo $hive_log | awk {'print $2'}`

#4比较2边的数字是否一样
if [ ${mysql_cnt} -eq ${hive_cnt} ] ; then \
echo "MySQL表的数据量有${mysql_cnt}条，hive表数据量有${hive_cnt}条，2边数据量一致" \
else \
echo "MySQL表的数据量有${mysql_cnt}条，hive表数据量有${hive_cnt}条，2边数据量不一致" \
fi
