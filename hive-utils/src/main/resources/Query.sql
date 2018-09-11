--SELECT [ALL | DISTINCT] select_expr, ...
--FROM table_name
--[WHERE where_condition]
--[GROUP BY col_list
-- | [DISTRIBUTE BY col_list] [SORT BY col_list]
-- | [ORDER BY col_list]]
--[LIMIT number]

--查询表结构
desc TABLE_NAME
--查询所有信息
select * from TABLE_NAME  --不转换成mapreduce作业
--支持算术表达式
select empno, ename, sal, sal*12 from emp;
--NULL值问题，表达式中一个变量为NULL，整个表达式为NULL，使用nvl()将NULL值转换成0
select empno, ename, sal, sal*12, comm, sal*12+nvl(comm, 0) from emp;
--查询奖金为NULL的员工，is为NULL，is not 不为NULL
select * from emp where comm is NULL;
--使用distinct去掉重复记录，distinct后有多个列时，多个列组合不重复；
select distinct depno from emp;

--Fetch Task功能，不开启mapreduce作业
--配置方式
  --set hive.fetch.task.conversion=more;
  --hive --hiveconf hive.fetch.task.conversion=more;
  --修改hive-site.xml文件

--查询名叫KING的员工
select * from emp where ename='KING';
select * from depno=10 and sal<2000;
--查看HQL的执行计划
explain HQL
--模糊查询，查询名字以S开头的员工，_ 匹配任意一个字符，% 匹配任意多个字符
select * from emp where ename like 'S%'
--查询名字中包含_的员工
select * from emp where ename like '%\\_%'

--排序，order by 后面 跟：列，表达式，别名，序号
select * from emp order by sal; --升序
select * from emp order by sal desc; --降序

select empno,ename,sal,sal*12 annsal from emp order by annsal;
set hive.groupby.orderby.position.alias=true;
select empno,ename,sal,sal*12 annsal from emp order by 4;
--NULL排序，升序：最前面  降序：最后面

--等值连接
 select e.empno, e.ename, e.sal, d.dname from emp e, dept d where e.deptno=d.deptno;
 --不等值连接
 select e.empno, e.ename, e.sal, s.grade from emp e, salgrade s where e.sal between s.losal and s.hisal;
--外连接
select d.deptno, d.dname, count(e.empno) from emp e, dept d where e.deptno=d.deptno group by d.deptno, d.dname;
--左外连接（当连接条件不成立时，连接条件左边的表依然包括在结果中），
--右外连接（当连接条件不成立时，连接条件右边的表依然包括在结果中）
select d.deptno, d.dname, count(e.empno) from emp e right outer join dept d on (e.deptno=d.deptno) group by d.deptno, d.dname;
--自连接，使用别名
select e.ename, b.ename from emp e, emp b where e.mgr=b.empno;

--子查询
select e.ename from emp e where e.deptno in (select d.deptno from dept d where d.dname='SALES' or d.dname='ACCOUNTING');
select * from emp e where e.empno not in (select mgr from emp e1 where e1.mgr is not null);