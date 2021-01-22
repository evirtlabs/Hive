Creating Database
-----------------------
Example1: Database Creation;

hive> CREATE DATABASE myhivetest; <br>
OK <br>
Time taken: 7.812 seconds <br>
hive> CREATE SCHEMA IF NOT EXISTS myhivetest; <br>
OK <br>
Time taken: 0.036 seconds <br>
hive> CREATE DATABASE IF NOT EXIST myhivetest <br>

HIVE HDFS TEST <br>
hive> CREATE DATABASE IF NOT EXISTS hdfstest <br>
    > COMMENT 'hive database demo' <br>
    > LOCATION '/opt/test' <br>
    > WITH DBPROPERTIES ('creator'='nidhi','date'='2020-03-01'); <br>
OK <br>
Time taken: 0.153 seconds  <br>
hive> <br>

-- To show the DDL use show create database since v2.1.0 <br>
hive> SHOW CREATE DATABASE default; <br>
OK <br>
CREATE DATABASE `default` <br>
COMMENT <br>
  'Default Hive database' <br>
LOCATION <br>
  'hdfs://localhost:9000/user/hive/warehouse' <br>
Time taken: 0.061 seconds, Fetched: 5 row(s) <br>
hive> <br>

hive> DESCRIBE DATABASE default; <br>
OK <br>
default Default Hive database   hdfs://localhost:9000/user/hive/warehouse      public   ROLE <br>
Time taken: 0.059 seconds, Fetched: 1 row(s) <br>
hive> <br>

-------------Table creation----- <br>

Show the data file content of employee.txt: <br>

cd /root/Hive_Labs/Lab1/data <br>
vim employee.txt <br>

Michael|Montreal,Toronto|Male,30|DB:80|Product:Developer^DLead <br>
Will|Montreal|Male,35|Perl:85|Product:Lead,Test:Lead <br>
Shelley|New York|Female,27|Python:80|Test:Lead,COE:Architect <br>
Lucy|Vancouver|Female,57|Sales:89,HR:94|Sales:Lead <br>

## Example:1 <br>
hive>  CREATE TABLE IF NOT EXISTS employee_internal ( <br>
    > name STRING COMMENT 'this is optinal column comments', <br>
    > work_place ARRAY<STRING>, <br>
    >  gender_age STRUCT<gender:STRING,age:INT>, <br>
    > skills_score MAP<STRING,INT>, <br>
    > depart_title MAP<STRING,ARRAY<STRING>> <br>
    > ); <br>
OK <br>
Time taken: 0.936 seconds <br>
hive> <br>

## Example: 2 <br>
hive> CREATE TABLE IF NOT EXISTS employee_internal ( <br>
    > name STRING COMMENT 'this is optinal column comments', <br>
    > work_place ARRAY<STRING>, <br>
    > gender_age STRUCT<gender:STRING,age:INT>, <br>
    > skills_score MAP<STRING,INT>, <br>
    > depart_title MAP<STRING,ARRAY<STRING>> <br>
    > )         <br>
    > COMMENT 'This is an internal table'          -- This is optional table  <br>
    > ROW FORMAT DELIMITED <br>
    > FIELDS TERMINATED BY '|'                      -- Symbol to seperate columns <br>
    > COLLECTION ITEMS TERMINATED BY ','           -- Seperate collection elements <br>
    > MAP KEYS TERMINATED BY ':'                   -- Symbol to seperate keys and values <br>
    > STORED as TEXTFILE; <br>
OK <br>
Time taken: 0.155 seconds <br>
hive> <br>

## Example3: Create an external table and load the data: <br>
hive> CREATE EXTERNAL TABLE employee_external ( <br>
    >  name string, <br>
    >  work_place ARRAY<string>, <br>
    > gender_age STRUCT<gender:string,age:int>, <br>
    >  skills_score MAP<string,int>, <br>
    > depart_title MAP<STRING,ARRAY<STRING>> <br>
    > ) <br>
    > ROW FORMAT DELIMITED <br>
    >  FIELDS TERMINATED BY '|' <br>
    > COLLECTION ITEMS TERMINATED BY ',' <br>
    >  MAP KEYS TERMINATED BY ':' <br>
    > STORED as TEXTFILE <br>
    > LOCATION '/root/Hive_Labs/Lab1/data/employee.txt';        ---Local Path (not hdfs) where file is kept <br>
OK <br>
Time taken: 0.228 seconds <br>
hive> <br>

hive> LOAD DATA INPATH '/opt/test/employee.txt'   ------hdfs path where employee.txt is kept <br>
    >  OVERWRITE INTO TABLE employee_external; <br>
Loading data to table default.employee_external <br>
OK <br>
Time taken: 1.05 seconds <br>
hive> <br>

hive> show tables; <br>
OK <br>
employee <br>
employee_external <br>
employee_internal <br>
names <br>
names_part <br>
names_text <br>
Time taken: 0.051 seconds, Fetched: 6 row(s) <br>
hive> describe employee_external; <br>
OK <br>
name                    string <br>
work_place              array<string> <br>
gender_age              struct<gender:string,age:int> <br>
skills_score            map<string,int> <br>
depart_title            map<string,array<string>> <br>
Time taken: 0.106 seconds, Fetched: 5 row(s) <br>
hive> select * from employee_external; <br>
OK <br>
Michael ["Montreal","Toronto"]  {"gender":"Male","age":30}      {"DB":80}      {"Product":["Developer","Lead"]} <br>
Will    ["Montreal"]    {"gender":"Male","age":35}      {"Perl":85}     {"Product":["Lead"],"Test":["Lead"]} <br>
Shelley ["New York"]    {"gender":"Female","age":27}    {"Python":80}   {"Test":["Lead"],"COE":["Architect"]} <br>
Lucy    ["Vancouver"]   {"gender":"Female","age":57}    {"Sales":89,"HR":94}   {"Sales":["Lead"]} <br>
Time taken: 1.61 seconds, Fetched: 4 row(s) <br>
hive> <br>

## Example 4: Creating temporary tables <br>

hive> CREATE TEMPORARY TABLE IF NOT EXISTS tmp_emp1 ( <br>
    >  name string, <br>
    >  work_place ARRAY<string>, <br>
    > gender_age STRUCT<gender:string,age:int>, <br>
    > skills_score MAP<string,int>, <br>
    > depart_title MAP<STRING,ARRAY<STRING>> <br>
    > ); <br>
OK <br>
Time taken: 0.081 seconds <br>

Creating temporary table from existing table <br>

hive> CREATE TEMPORARY TABLE tmp_emp2 as SELECT * FROM tmp_emp1; <br>

WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases. <br>
Query ID = root_20200902202347_44e0225f-ac63-4245-9f6a-173db805da76 <br>
Total jobs = 3 <br>
Launching Job 1 out of 3 <br>
Number of reduce tasks is set to 0 since there's no reduce operator <br>
Starting Job = job_1599083139145_0001, Tracking URL = http://hadoop:8088/proxy/application_1599083139145_0001/ <br>
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1599083139145_0001 <br>
Hadoop job information for Stage-1: number of mappers: 0; number of reducers: 0 <br>
2020-09-02 20:24:07,244 Stage-1 map = 0%,  reduce = 0% <br>
Ended Job = job_1599083139145_0001 <br>
Stage-4 is selected by condition resolver. <br>
Stage-3 is filtered out by condition resolver. <br>
Stage-5 is filtered out by condition resolver. <br>
Moving data to directory hdfs://localhost:9000/tmp/hive/root/c37bad42-dc1e-48f7-8f1f-dee926a557d7/_tmp_space.db/cb1f3590-14eb-45a7-8371-ea50fcf37312/.hive-staging_hive_2020-09-02_20-23-47_079_1654893301774554362-1/-ext-10002 <br>
Moving data to directory hdfs://localhost:9000/tmp/hive/root/c37bad42-dc1e-48f7-8f1f-dee926a557d7/_tmp_space.db/cb1f3590-14eb-45a7-8371-ea50fcf37312
MapReduce Jobs Launched: <br>
Stage-Stage-1:  HDFS Read: 0 HDFS Write: 0 SUCCESS <br>
Total MapReduce CPU Time Spent: 0 msec <br>
OK <br>
Time taken: 23.477 seconds <br>
hive> <br>

## Example5: Tables can also be created and populated by the results of a query in one statement, called Create-Table-As-Select (CTAS). <br>
hive>  CREATE TABLE ctas_employee as SELECT * FROM employee_external; <br>
hive> describe ctas_employee; <br>
OK <br>
name                    string <br>
work_place              array<string> <br>
gender_age              struct<gender:string,age:int> <br>
skills_score            map<string,int> <br>
depart_title            map<string,array<string>> <br>
Time taken: 0.146 seconds, Fetched: 5 row(s) <br>
hive> select * from ctas_employee; <br>
OK <br>
Michael ["Montreal","Toronto"]  {"gender":"Male","age":30}      {"DB":80}       {"Product":["Developer","Lead"]} <br>
Will    ["Montreal"]    {"gender":"Male","age":35}      {"Perl":85}     {"Product":["Lead"],"Test":["Lead"]} <br>
Shelley ["New York"]    {"gender":"Female","age":27}    {"Python":80}   {"Test":["Lead"],"COE":["Architect"]} <br>
Lucy    ["Vancouver"]   {"gender":"Female","age":57}    {"Sales":89,"HR":94}    {"Sales":["Lead"]} <br>
Time taken: 0.406 seconds, Fetched: 4 row(s) <br>
hive> <br>

## Example6: Complex table creation <br>

hive> CREATE TABLE cte_employee as <br>
    > WITH r1 as ( <br>
    > SELECT name FROM r2 WHERE name = 'Michael' <br>
    > ), <br>
    > r2 as ( <br>
    >  SELECT name FROM employee WHERE gender_age.gender= 'Male' <br>
    > ), <br>
    > r3 as ( <br>
    > SELECT name FROM employee WHERE gender_age.gender= 'Female' <br>
    > ) <br>
    > SELECT * FROM r1 <br>
    > UNION ALL <br>
    >  SELECT * FROM r3; <br>

hive> describe  cte_employee; <br>
OK <br>
name                    string <br>
Time taken: 0.097 seconds, Fetched: 1 row(s) <br>


hive> select * from cte_employee; <br>
OK <br>
Michael <br>
Shelley <br>
Lucy <br>
Time taken: 0.235 seconds, Fetched: 3 row(s) <br>
hive> <br>
 
## Example7: Create Partitioned table <br>

hive> CREATE TABLE employee_partitioned ( <br>
    > name STRING, <br>
    > work_place ARRAY<STRING>, <br>
    > gender_age STRUCT<gender:STRING,age:INT>, <br>
    > skills_score MAP<STRING,INT>, <br>
    >  depart_title MAP<STRING,ARRAY<STRING>> <br>
    > ) <br>
    >  PARTITIONED BY (year INT, month INT) <br>
    > ROW FORMAT DELIMITED <br>
    > FIELDS TERMINATED BY '|' <br>
    > COLLECTION ITEMS TERMINATED BY ',' <br>
    > MAP KEYS TERMINATED BY ':'; <br>
OK <br>
Time taken: 7.953 seconds <br>
hive> <br>

hive>  DESC employee_partitioned; <br>
OK <br>
name                    string <br>
work_place              array<string> <br>
gender_age              struct<gender:string,age:int> <br>
skills_score            map<string,int> <br>
depart_title            map<string,array<string>> <br>
year                    int <br>
month                   int <br>

# Partition Information <br>
# col_name              data_type               comment <br>

year                    int <br>
month                   int <br>
Time taken: 0.595 seconds, Fetched: 13 row(s) <br>
hive> <br>

Perform partition operations, such as add, remove, and rename partitions <br>

hive> ALTER TABLE employee_partitioned ADD <br>
    > PARTITION (year=2018, month=11) PARTITION (year=2018, <br>
    >       month=12); <br>
OK <br>
Time taken: 0.475 seconds <br>
hive> <br>

hive> SHOW PARTITIONS employee_partitioned; <br>
OK <br>
year=2018/month=11 <br>
year=2018/month=12 <br>
Time taken: 0.244 seconds, Fetched: 2 row(s) <br>
hive> <br>

      -- Drop partition with PURGE at the end will remove completely <br>
      -- Drop partition will NOT remove data for external table <br>
      -- Drop partition will remove data with partition for internal table <br>

hive> ALTER TABLE employee_partitioned <br>
    >  DROP IF EXISTS PARTITION (year=2018, month=11); <br>
Dropped the partition year=2018/month=11 <br>
OK <br>
Time taken: 0.933 seconds <br>
hive> <br>


hive> SHOW PARTITIONS employee_partitioned; <br>
OK <br>
year=2018/month=12 <br>
Time taken: 0.227 seconds, Fetched: 1 row(s) <br>
hive> <br>


hive> ALTER TABLE employee_partitioned <br>
    > DROP IF EXISTS PARTITION (year=2017); <br>
OK <br>
Time taken: 0.243 seconds <br>


hive> ALTER TABLE employee_partitioned <br>
    > DROP IF EXISTS PARTITION (month=9); <br>
OK <br>
Time taken: 0.251 seconds <br>


hive> ALTER TABLE employee_partitioned <br>
    > PARTITION (year=2018, month=12) <br>
    > RENAME TO PARTITION (year=2018,month=10); <br>
OK <br>
Time taken: 0.575 seconds <br>
hive> <br>

hive> SHOW PARTITIONS employee_partitioned; <br>
OK <br>
year=2018/month=10 <br>
Time taken: 0.175 seconds, Fetched: 1 row(s) <br>
hive> <br>

Load data into a table partition once the partition is created: <br>
    
hive>  LOAD DATA INPATH '/root/Hive_Labs/Lab1/data/employee.txt' <br>
    > OVERWRITE INTO TABLE employee_partitioned <br>
    > PARTITION (year=2018, month=12); <br>
Loading data to table default.employee_partitioned partition (year=2018, month=12) <br>
OK <br>
Time taken: 1.122 seconds <br>
hive> <br>


hive> SELECT name, year, month FROM employee_partitioned; <br>
OK <br>
Michael 2018    12 <br>
Will    2018    12 <br>
Shelley 2018    12 <br>
Lucy    2018    12 <br>
Time taken: 1.91 seconds, Fetched: 4 row(s) <br>
hive> <br>


Add regular columns to a partition table. Note, we CANNOT add new columns as partition columns. There are two options when adding/removing columns from a partition table, <br>  CASCADE and RESTRICT. The commonly used CASCADE option cascades the same change to all the partitions in the table. However,  RESTRICT is the default, limiting column <br> changes only to table metadata, which means the changes will be only applied to new partitions rather than existing partitions: <br>

hive> ALTER TABLE employee_partitioned ADD COLUMNS (work string) <br>
    > CASCADE; <br>
OK <br>
Time taken: 7.167 seconds <br>
hive>  ALTER TABLE employee_partitioned PARTITION COLUMN(year string); <br>
OK <br>
Time taken: 0.345 seconds <br>
hive> DESC employee_partitioned; <br>
OK <br>
name                    string <br>
work_place              array<string> <br>
gender_age              struct<gender:string,age:int> <br>
skills_score            map<string,int> <br>
depart_title            map<string,array<string>> <br>
work                    string <br>
year                    string <br>
month                   int <br>

# Partition Information <br>
# col_name              data_type               comment <br>

year                    string <br>
month                   int <br>
Time taken: 0.717 seconds, Fetched: 14 row(s) <br>
hive> <br>



    





