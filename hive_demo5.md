#Complex Queries in Hive

1. Upload all the data in hdfs
-------------------------------
(base) [root@hadoop data]# ls
employee_contract.txt  employee_hr.txt  employee_id.txt  employee.txt
(base) [root@hadoop data]# hadoop fs -mkdir /opt/hivelabs
(base) [root@hadoop data]# hadoop fs -put *.txt /opt/hivelabs
(base) [root@hadoop data]# hadoop fs -ls /opt/hivelabs
Found 4 items
-rw-r--r--   1 root supergroup        227 2020-09-03 21:00 /opt/hivelabs/employee.txt
-rw-r--r--   1 root supergroup        391 2020-09-03 21:00 /opt/hivelabs/employee_contract.txt
-rw-r--r--   1 root supergroup        133 2020-09-03 21:00 /opt/hivelabs/employee_hr.txt
-rw-r--r--   1 root supergroup       1473 2020-09-03 21:00 /opt/hivelabs/employee_id.txt
(base) [root@hadoop data]#

hive> CREATE TABLE employee_id
    > (
    > name string,
    > employee_id int,
    > work_place ARRAY<string>,
    > sex_age STRUCT<sex:string,age:int>,
    > skills_score MAP<string,int>,
    > depart_title MAP<string,ARRAY<string>>
    > )
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY '|'
    > COLLECTION ITEMS TERMINATED BY ','
    > MAP KEYS TERMINATED BY ':';
OK
Time taken: 7.524 seconds


hive> LOAD DATA INPATH '/opt/hivelabs/employee_id.txt'
    > OVERWRITE INTO TABLE employee_id;
Loading data to table default.employee_id
OK
Time taken: 0.951 seconds
hive>

************************************************

Bucket
-------
bucket is another technique to cluster datasets into more manageable parts to optimize query performance. Different from partition, the bucket corresponds to
segments of files in HDFS. Bucket numbers are used to define the proper number of buckets, we should avoid having too much or too little of data in each bucket.
A better choice is somewhere near two blocks of data. For example, we can plan 512 MB of data in each bucket, if the Hadoop block size is 256 MB. If possible, use
2N as the number of buckets.

hive> CREATE TABLE employee_id_buckets
    > (
    > name string,
    > employee_id int,
    > work_place ARRAY<string>,
    > sex_age STRUCT<sex:string,age:int>,
    > skills_score MAP<string,int>,
    > depart_title MAP<string,ARRAY<string >>
    > )
    > CLUSTERED BY (employee_id) INTO 2 BUCKETS
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY '|'
    > COLLECTION ITEMS TERMINATED BY ','
    > MAP KEYS TERMINATED BY ':';
OK
Time taken: 0.199 seconds
hive>


hive> set map.reduce.tasks = 2;
hive> set hive.enforce.bucketing = true;

hive> INSERT OVERWRITE TABLE employee_id_buckets
    > SELECT * FROM employee_id;

WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20200903213900_3acf321a-1bdb-434d-8902-8fe9d2176ea2
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 2
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1599083139145_0007, Tracking URL = http://hadoop:8088/proxy/application_1599083139145_0007/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1599083139145_0007
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 2
2020-09-03 21:39:14,340 Stage-1 map = 0%,  reduce = 0%
2020-09-03 21:39:21,820 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 1.23 sec
2020-09-03 21:39:30,237 Stage-1 map = 100%,  reduce = 50%, Cumulative CPU 2.69 sec
2020-09-03 21:40:31,070 Stage-1 map = 100%,  reduce = 50%, Cumulative CPU 2.69 sec
2020-09-03 21:41:31,382 Stage-1 map = 100%,  reduce = 50%, Cumulative CPU 2.69 sec
2020-09-03 21:42:31,607 Stage-1 map = 100%,  reduce = 50%, Cumulative CPU 2.69 sec
2020-09-03 21:42:32,622 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 4.5 sec
MapReduce Total cumulative CPU time: 4 seconds 500 msec
Ended Job = job_1599083139145_0007
Loading data to table default.employee_id_buckets
MapReduce Jobs Launched:
Stage-Stage-1: Map: 1  Reduce: 2   Cumulative CPU: 4.5 sec   HDFS Read: 18169 HDFS Write: 1641 SUCCESS
Total MapReduce CPU Time Spent: 4 seconds 500 msec
OK
Time taken: 213.574 seconds
hive>


(base) [root@hadoop data]# hadoop fs -ls /user/hive/warehouse/employee_id_buckets
Found 2 items
-rwxrwxr-x   1 root supergroup        769 2020-09-03 21:39 /user/hive/warehouse/employee_id_buckets/000000_0
-rwxrwxr-x   1 root supergroup        704 2020-09-03 21:42 /user/hive/warehouse/employee_id_buckets/000001_1
(base) [root@hadoop data]#

***********************************************
HIVE VIEWS
------------
In Hive, views are logical data structures that can be used to simplify queries by either hiding the complexities such as joins, subqueries, and filters or by flatting the data. Unlike some RDBMS, Hive views do not store data or get materialized. Once the Hive view is created, its schema is frozen immediately. Subsequent changes to the underlying tables (for example, adding a column) will not be reflected in the view’s schema.

hive> CREATE VIEW employee_skills
    > AS
    > SELECT name, skills_score['DB'] AS DB,
    > skills_score['Perl'] AS Perl,
    > skills_score['Python'] AS Python,
    > skills_score['Sales'] as Sales,
    > skills_score['HR'] as HR
    > FROM employee;
OK
Time taken: 10.004 seconds
hive>

Alter the views’ properties:
------------------------------
hive> ALTER VIEW employee_skills
    > SET TBLPROPERTIES ('comment' = 'This is a view');
OK
Time taken: 0.231 seconds

Redefine views:
-----------------------

hive> ALTER VIEW employee_skills AS
    > SELECT * from employee ;
OK
Time taken: 0.416 seconds

Drop views:
------------------

hive> DROP VIEW employee_skills;
OK
Time taken: 0.416 seconds
hive>






