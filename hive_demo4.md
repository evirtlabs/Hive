Creating Database
-----------------------
Example1: Database Creation;

hive> CREATE DATABASE myhivetest;
OK
Time taken: 7.812 seconds
hive> CREATE SCHEMA IF NOT EXISTS myhivetest;
OK
Time taken: 0.036 seconds
hive> CREATE DATABASE IF NOT EXIST myhivetest

HIVE HDFS TEST
hive> CREATE DATABASE IF NOT EXISTS hdfstest
    > COMMENT 'hive database demo'
    > LOCATION '/opt/test'
    > WITH DBPROPERTIES ('creator'='nidhi','date'='2020-03-01');
OK
Time taken: 0.153 seconds
hive>

-- To show the DDL use show create database since v2.1.0
hive> SHOW CREATE DATABASE default;
OK
CREATE DATABASE `default`
COMMENT
  'Default Hive database'
LOCATION
  'hdfs://localhost:9000/user/hive/warehouse'
Time taken: 0.061 seconds, Fetched: 5 row(s)
hive>

hive> DESCRIBE DATABASE default;
OK
default Default Hive database   hdfs://localhost:9000/user/hive/warehouse      public   ROLE
Time taken: 0.059 seconds, Fetched: 1 row(s)
hive>

-------------Table creation-----

Show the data file content of employee.txt:

cd /root/Hive_Labs/Lab1/data
vim employee.txt

Michael|Montreal,Toronto|Male,30|DB:80|Product:Developer^DLead
Will|Montreal|Male,35|Perl:85|Product:Lead,Test:Lead
Shelley|New York|Female,27|Python:80|Test:Lead,COE:Architect
Lucy|Vancouver|Female,57|Sales:89,HR:94|Sales:Lead

## Example:1
hive>  CREATE TABLE IF NOT EXISTS employee_internal (
    > name STRING COMMENT 'this is optinal column comments',
    > work_place ARRAY<STRING>,
    >  gender_age STRUCT<gender:STRING,age:INT>,
    > skills_score MAP<STRING,INT>,
    > depart_title MAP<STRING,ARRAY<STRING>>
    > );
OK
Time taken: 0.936 seconds
hive>

## Example: 2
hive> CREATE TABLE IF NOT EXISTS employee_internal (
    > name STRING COMMENT 'this is optinal column comments',
    > work_place ARRAY<STRING>,
    > gender_age STRUCT<gender:STRING,age:INT>,
    > skills_score MAP<STRING,INT>,
    > depart_title MAP<STRING,ARRAY<STRING>>
    > )        
    > COMMENT 'This is an internal table'          -- This is optional table 
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY '|'                      -- Symbol to seperate columns
    > COLLECTION ITEMS TERMINATED BY ','           -- Seperate collection elements
    > MAP KEYS TERMINATED BY ':'                   -- Symbol to seperate keys and values
    > STORED as TEXTFILE;
OK
Time taken: 0.155 seconds
hive>

## Example3: Create an external table and load the data:
hive> CREATE EXTERNAL TABLE employee_external (
    >  name string,
    >  work_place ARRAY<string>,
    > gender_age STRUCT<gender:string,age:int>,
    >  skills_score MAP<string,int>,
    > depart_title MAP<STRING,ARRAY<STRING>>
    > )
    > ROW FORMAT DELIMITED
    >  FIELDS TERMINATED BY '|'
    > COLLECTION ITEMS TERMINATED BY ','
    >  MAP KEYS TERMINATED BY ':'
    > STORED as TEXTFILE
    > LOCATION '/root/Hive_Labs/Lab1/data/employee.txt';        ---Local Path (not hdfs) where file is kept
OK
Time taken: 0.228 seconds
hive>

hive> LOAD DATA INPATH '/opt/test/employee.txt'   ------hdfs path where employee.txt is kept
    >  OVERWRITE INTO TABLE employee_external;
Loading data to table default.employee_external
OK
Time taken: 1.05 seconds
hive>

hive> show tables;
OK
employee
employee_external
employee_internal
names
names_part
names_text
Time taken: 0.051 seconds, Fetched: 6 row(s)
hive> describe employee_external;
OK
name                    string
work_place              array<string>
gender_age              struct<gender:string,age:int>
skills_score            map<string,int>
depart_title            map<string,array<string>>
Time taken: 0.106 seconds, Fetched: 5 row(s)
hive> select * from employee_external;
OK
Michael ["Montreal","Toronto"]  {"gender":"Male","age":30}      {"DB":80}      {"Product":["Developer","Lead"]}
Will    ["Montreal"]    {"gender":"Male","age":35}      {"Perl":85}     {"Product":["Lead"],"Test":["Lead"]}
Shelley ["New York"]    {"gender":"Female","age":27}    {"Python":80}   {"Test":["Lead"],"COE":["Architect"]}
Lucy    ["Vancouver"]   {"gender":"Female","age":57}    {"Sales":89,"HR":94}   {"Sales":["Lead"]}
Time taken: 1.61 seconds, Fetched: 4 row(s)
hive>

## Example 4: Creating temporary tables

hive> CREATE TEMPORARY TABLE IF NOT EXISTS tmp_emp1 (
    >  name string,
    >  work_place ARRAY<string>,
    > gender_age STRUCT<gender:string,age:int>,
    > skills_score MAP<string,int>,
    > depart_title MAP<STRING,ARRAY<STRING>>
    > );
OK
Time taken: 0.081 seconds

Creating temporary table from existing table

hive> CREATE TEMPORARY TABLE tmp_emp2 as SELECT * FROM tmp_emp1;

WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20200902202347_44e0225f-ac63-4245-9f6a-173db805da76
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1599083139145_0001, Tracking URL = http://hadoop:8088/proxy/application_1599083139145_0001/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1599083139145_0001
Hadoop job information for Stage-1: number of mappers: 0; number of reducers: 0
2020-09-02 20:24:07,244 Stage-1 map = 0%,  reduce = 0%
Ended Job = job_1599083139145_0001
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to directory hdfs://localhost:9000/tmp/hive/root/c37bad42-dc1e-48f7-8f1f-dee926a557d7/_tmp_space.db/cb1f3590-14eb-45a7-8371-ea50fcf37312/.hive-staging_hive_2020-09-02_20-23-47_079_1654893301774554362-1/-ext-10002
Moving data to directory hdfs://localhost:9000/tmp/hive/root/c37bad42-dc1e-48f7-8f1f-dee926a557d7/_tmp_space.db/cb1f3590-14eb-45a7-8371-ea50fcf37312
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 0 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
Time taken: 23.477 seconds
hive>

## Example5: Tables can also be created and populated by the results of a query in one statement, called Create-Table-As-Select (CTAS).
hive>  CREATE TABLE ctas_employee as SELECT * FROM employee_external;
hive> describe ctas_employee;
OK
name                    string
work_place              array<string>
gender_age              struct<gender:string,age:int>
skills_score            map<string,int>
depart_title            map<string,array<string>>
Time taken: 0.146 seconds, Fetched: 5 row(s)
hive> select * from ctas_employee;
OK
Michael ["Montreal","Toronto"]  {"gender":"Male","age":30}      {"DB":80}       {"Product":["Developer","Lead"]}
Will    ["Montreal"]    {"gender":"Male","age":35}      {"Perl":85}     {"Product":["Lead"],"Test":["Lead"]}
Shelley ["New York"]    {"gender":"Female","age":27}    {"Python":80}   {"Test":["Lead"],"COE":["Architect"]}
Lucy    ["Vancouver"]   {"gender":"Female","age":57}    {"Sales":89,"HR":94}    {"Sales":["Lead"]}
Time taken: 0.406 seconds, Fetched: 4 row(s)
hive>

## Example6: Complex table creation

hive> CREATE TABLE cte_employee as
    > WITH r1 as (
    > SELECT name FROM r2 WHERE name = 'Michael'
    > ),
    > r2 as (
    >  SELECT name FROM employee WHERE gender_age.gender= 'Male'
    > ),
    > r3 as (
    > SELECT name FROM employee WHERE gender_age.gender= 'Female'
    > )
    > SELECT * FROM r1
    > UNION ALL
    >  SELECT * FROM r3;

hive> describe  cte_employee;
OK
name                    string
Time taken: 0.097 seconds, Fetched: 1 row(s)


hive> select * from cte_employee;
OK
Michael
Shelley
Lucy
Time taken: 0.235 seconds, Fetched: 3 row(s)
hive>

## Example7: Create Partitioned table

hive> CREATE TABLE employee_partitioned (
    > name STRING,
    > work_place ARRAY<STRING>,
    > gender_age STRUCT<gender:STRING,age:INT>,
    > skills_score MAP<STRING,INT>,
    >  depart_title MAP<STRING,ARRAY<STRING>>
    > )
    >  PARTITIONED BY (year INT, month INT)
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY '|'
    > COLLECTION ITEMS TERMINATED BY ','
    > MAP KEYS TERMINATED BY ':';
OK
Time taken: 7.953 seconds
hive>

hive>  DESC employee_partitioned;
OK
name                    string
work_place              array<string>
gender_age              struct<gender:string,age:int>
skills_score            map<string,int>
depart_title            map<string,array<string>>
year                    int
month                   int

# Partition Information
# col_name              data_type               comment

year                    int
month                   int
Time taken: 0.595 seconds, Fetched: 13 row(s)
hive>

Perform partition operations, such as add, remove, and rename partitions

hive> ALTER TABLE employee_partitioned ADD
    > PARTITION (year=2018, month=11) PARTITION (year=2018,
    >       month=12);
OK
Time taken: 0.475 seconds
hive>

hive> SHOW PARTITIONS employee_partitioned;
OK
year=2018/month=11
year=2018/month=12
Time taken: 0.244 seconds, Fetched: 2 row(s)
hive>

      -- Drop partition with PURGE at the end will remove completely
      -- Drop partition will NOT remove data for external table
      -- Drop partition will remove data with partition for internal table

hive> ALTER TABLE employee_partitioned
    >  DROP IF EXISTS PARTITION (year=2018, month=11);
Dropped the partition year=2018/month=11
OK
Time taken: 0.933 seconds
hive>


hive> SHOW PARTITIONS employee_partitioned;
OK
year=2018/month=12
Time taken: 0.227 seconds, Fetched: 1 row(s)
hive>


hive> ALTER TABLE employee_partitioned
    > DROP IF EXISTS PARTITION (year=2017);
OK
Time taken: 0.243 seconds


hive> ALTER TABLE employee_partitioned
    > DROP IF EXISTS PARTITION (month=9);
OK
Time taken: 0.251 seconds


hive> ALTER TABLE employee_partitioned
    > PARTITION (year=2018, month=12)
    > RENAME TO PARTITION (year=2018,month=10);
OK
Time taken: 0.575 seconds
hive>

hive> SHOW PARTITIONS employee_partitioned;
OK
year=2018/month=10
Time taken: 0.175 seconds, Fetched: 1 row(s)
hive>

Load data into a table partition once the partition is created:
    
hive>  LOAD DATA INPATH '/root/Hive_Labs/Lab1/data/employee.txt'
    > OVERWRITE INTO TABLE employee_partitioned
    > PARTITION (year=2018, month=12);
Loading data to table default.employee_partitioned partition (year=2018, month=12)
OK
Time taken: 1.122 seconds
hive>


hive> SELECT name, year, month FROM employee_partitioned;
OK
Michael 2018    12
Will    2018    12
Shelley 2018    12
Lucy    2018    12
Time taken: 1.91 seconds, Fetched: 4 row(s)
hive>


Add regular columns to a partition table. Note, we CANNOT add new columns as partition columns. There are two options when adding/removing columns from a partition table, CASCADE and RESTRICT. The commonly used CASCADE option cascades the same change to all the partitions in the table. However,  RESTRICT is the default, limiting column changes only to table metadata, which means the changes will be only applied to new partitions rather than existing partitions:

hive> ALTER TABLE employee_partitioned ADD COLUMNS (work string)
    > CASCADE;
OK
Time taken: 7.167 seconds
hive>  ALTER TABLE employee_partitioned PARTITION COLUMN(year string);
OK
Time taken: 0.345 seconds
hive> DESC employee_partitioned;
OK
name                    string
work_place              array<string>
gender_age              struct<gender:string,age:int>
skills_score            map<string,int>
depart_title            map<string,array<string>>
work                    string
year                    string
month                   int

# Partition Information
# col_name              data_type               comment

year                    string
month                   int
Time taken: 0.717 seconds, Fetched: 14 row(s)
hive>



    





