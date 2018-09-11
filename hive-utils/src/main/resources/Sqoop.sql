--使用Sqoop导入导出关系型数据库中的数据到hdfs或hive数据仓库中

--使用Sqoop导入Oracle数据到HDFS中
./sqoop import --connect jdbc:oracle:thin:@192.168.0.1:1521:database --username root --password 123456
--table emp --columns 'empno,ename,job,sal,depno' -m 1 --target-dir '/sqoop/emp'

--使用Sqoop导入Oracle数据到Hive中
./sqoop import --hive-import --connect jdbc:oracle:thin:@192.168.0.1:1521:database --username root --password 123456
--table emp --columns 'empno,ename,job,sal,depno' -m 1

--使用Sqoop导入Oracle数据到Hive中，并且制定表名
./sqoop import --hive-import --connect jdbc:oracle:thin:@192.168.0.1:1521:database --username root --password 123456
--table emp --columns 'empno,ename,job,sal,depno' -m 1 --hive-table emp1

--使用Sqoop导入Oracle数据到Hive中，并使用where条件
./sqoop import --hive-import --connect jdbc:oracle:thin:@192.168.0.1:1521:database --username root --password 123456
--table emp --columns 'empno,ename,job,sal,depno' -m 1 --hive-table emp1 --where 'depno=10'

--使用Sqoop导入Oracle数据到Hive中，并使用查询语句
./sqoop import --hive-import --connect jdbc:oracle:thin:@192.168.0.1:1521:database --username root --password 123456
 -m 1 --query 'SELECT * FROM EMP WHERE SAL<2000 AND $CONDITIONS' --target-dir '/sqoop/emp5' --hive-table emp5

--使用Sqoop将Hive中的数据导出到Oracle中，需先在Oracle中创建相应的表
./sqoop exprot --connect jdbc:oracle:thin:@192.168.0.1:1521:database --username root --password 123456
 -m 1 --table MYEMP --export-dir <路径>