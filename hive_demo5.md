# Complex Queries in Hive <br>

1. Upload all the data in hdfs <br>
------------------------------- <br>
(base) [root@hadoop data]# ls <br>
employee_contract.txt  employee_hr.txt  employee_id.txt  employee.txt <br>
(base) [root@hadoop data]# hadoop fs -mkdir /opt/hivelabs <br>
(base) [root@hadoop data]# hadoop fs -put *.txt /opt/hivelabs <br>
(base) [root@hadoop data]# hadoop fs -ls /opt/hivelabs <br>
Found 4 items <br>
-rw-r--r--   1 root supergroup        227 2020-09-03 21:00 /opt/hivelabs/employee.txt <br>
-rw-r--r--   1 root supergroup        391 2020-09-03 21:00 /opt/hivelabs/employee_contract.txt <br>
-rw-r--r--   1 root supergroup        133 2020-09-03 21:00 /opt/hivelabs/employee_hr.txt <br>
-rw-r--r--   1 root supergroup       1473 2020-09-03 21:00 /opt/hivelabs/employee_id.txt <br>
(base) [root@hadoop data]# <br>

hive> CREATE TABLE employee_id <br>
    > ( <br>
    > name string, <br>
    > employee_id int, <br>
    > work_place ARRAY<string>, <br>
    > sex_age STRUCT<sex:string,age:int>, <br>
    > skills_score MAP<string,int>, <br>
    > depart_title MAP<string,ARRAY<string>> <br>
    > ) <br>
    > ROW FORMAT DELIMITED <br>
    > FIELDS TERMINATED BY '|' <br>
    > COLLECTION ITEMS TERMINATED BY ',' <br>
    > MAP KEYS TERMINATED BY ':'; <br>
OK <br>
Time taken: 7.524 seconds <br>


hive> LOAD DATA INPATH '/opt/hivelabs/employee_id.txt' <br>
    > OVERWRITE INTO TABLE employee_id; <br>
Loading data to table default.employee_id <br>
OK <br>
Time taken: 0.951 seconds <br>
hive> <br>

************************************************ <br>

## Bucket <br>
------- <br>
bucket is another technique to cluster datasets into more manageable parts to optimize query performance. Different from partition, the bucket corresponds to <br>
segments of files in HDFS. Bucket numbers are used to define the proper number of buckets, we should avoid having too much or too little of data in each bucket. <br>
A better choice is somewhere near two blocks of data. For example, we can plan 512 MB of data in each bucket, if the Hadoop block size is 256 MB. If possible, use <br>
2N as the number of buckets. <br>

hive> CREATE TABLE employee_id_buckets <br>
    > ( <br>
    > name string, <br>
    > employee_id int, <br>
    > work_place ARRAY<string>, <br>
    > sex_age STRUCT<sex:string,age:int>, <br>
    > skills_score MAP<string,int>, <br>
    > depart_title MAP<string,ARRAY<string >> <br>
    > ) <br>
    > CLUSTERED BY (employee_id) INTO 2 BUCKETS <br>
    > ROW FORMAT DELIMITED <br>
    > FIELDS TERMINATED BY '|' <br>
    > COLLECTION ITEMS TERMINATED BY ',' <br>
    > MAP KEYS TERMINATED BY ':'; <br>
OK <br>
Time taken: 0.199 seconds <br>
hive> <br>


hive> set map.reduce.tasks = 2; <br>
hive> set hive.enforce.bucketing = true; <br>

hive> INSERT OVERWRITE TABLE employee_id_buckets <br>
    > SELECT * FROM employee_id; <br>

WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X  releases. <br>
Query ID = root_20200903213900_3acf321a-1bdb-434d-8902-8fe9d2176ea2 <br>
Total jobs = 1 <br>
Launching Job 1 out of 1 <br>
Number of reduce tasks determined at compile time: 2 <br>
In order to change the average load for a reducer (in bytes): <br>
  set hive.exec.reducers.bytes.per.reducer=<number> <br>
In order to limit the maximum number of reducers: <br>
  set hive.exec.reducers.max=<number> <br>
In order to set a constant number of reducers: <br>
  set mapreduce.job.reduces=<number> <br>
Starting Job = job_1599083139145_0007, Tracking URL = http://hadoop:8088/proxy/application_1599083139145_0007/ <br>
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1599083139145_0007 <br>
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 2 <br>
2020-09-03 21:39:14,340 Stage-1 map = 0%,  reduce = 0% <br>
2020-09-03 21:39:21,820 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.23 sec <br>
2020-09-03 21:39:30,237 Stage-1 map = 100%,  reduce = 50%, Cumulative CPU 2.69 sec <br>
2020-09-03 21:40:31,070 Stage-1 map = 100%,  reduce = 50%, Cumulative CPU 2.69 sec <br>
2020-09-03 21:41:31,382 Stage-1 map = 100%,  reduce = 50%, Cumulative CPU 2.69 sec <br>
2020-09-03 21:42:31,607 Stage-1 map = 100%,  reduce = 50%, Cumulative CPU 2.69 sec <br>
2020-09-03 21:42:32,622 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 4.5 sec <br>
MapReduce Total cumulative CPU time: 4 seconds 500 msec <br>
Ended Job = job_1599083139145_0007 <br>
Loading data to table default.employee_id_buckets <br>
MapReduce Jobs Launched: <br>
Stage-Stage-1: Map: 1  Reduce: 2   Cumulative CPU: 4.5 sec   HDFS Read: 18169 HDFS Write: 1641 SUCCESS <br>
Total MapReduce CPU Time Spent: 4 seconds 500 msec <br>
OK <br>
Time taken: 213.574 seconds <br>
hive> <br>


(base) [root@hadoop data]# hadoop fs -ls /user/hive/warehouse/employee_id_buckets <br>
Found 2 items <br>
-rwxrwxr-x   1 root supergroup        769 2020-09-03 21:39 /user/hive/warehouse/employee_id_buckets/000000_0 <br>
-rwxrwxr-x   1 root supergroup        704 2020-09-03 21:42 /user/hive/warehouse/employee_id_buckets/000001_1 <br>
(base) [root@hadoop data]# <br>

*********************************************** <br>
## HIVE VIEWS <br>
------------
In Hive, views are logical data structures that can be used to simplify queries by either hiding the complexities such as joins, subqueries, and filters or by flatting the  data. Unlike some RDBMS, Hive views do not store data or get materialized. Once the Hive view is created, its schema is frozen immediately. Subsequent changes to the underlying tables (for example, adding a column) will not be reflected in the view’s schema. <br>

hive> CREATE VIEW employee_skills <br>
    > AS <br>
    > SELECT name, skills_score['DB'] AS DB, <br>
    > skills_score['Perl'] AS Perl, <br>
    > skills_score['Python'] AS Python, <br>
    > skills_score['Sales'] as Sales, <br>
    > skills_score['HR'] as HR <br>
    > FROM employee; <br>
OK <br>
Time taken: 10.004 seconds <br>
hive> <br>

Alter the views’ properties: <br>
------------------------------
hive> ALTER VIEW employee_skills <br>
    > SET TBLPROPERTIES ('comment' = 'This is a view'); <br>
OK <br>
Time taken: 0.231 seconds <br>

Redefine views: <br>
----------------------- <br>

hive> ALTER VIEW employee_skills AS <br>
    > SELECT * from employee ; <br>
OK <br>
Time taken: 0.416 seconds <br>

Drop views: <br>
------------------ <br>

hive> DROP VIEW employee_skills; <br>
OK <br>
Time taken: 0.416 seconds <br>
hive> <br>






