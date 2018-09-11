--Load从本地或hdfs导入数据到hive
--load data [local] inpath 'filepath' [overwrite | into] table tablename [partition (partcol1=val1, partcol2=val2 ...)]
--从本地导入数据
load data local inpath '/tmp/student.txt' into table student;
--将目录下的所有数据文件导入hive
load data local inpath '/tmp/' overwrite into table student;
--将hdfs上的数据导入hive
load data inpath '/input/student.txt' overwrite into table student;
--将数据导入分区表中
load data local inpath '/tmp/data.txt' into table partition_table partition (gender='M');