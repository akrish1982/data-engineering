#### create resources using this stack
https://console.aws.amazon.com/cloudformation/home?#/stacks/new?stackName=RedshiftImmersionLab&templateURL=https://s3-us-west-2.amazonaws.com/redshift-immersionday-labs/immersionserverless.yaml

#### To connect use
User Name: awsuser
Password: Awsuser123

#### TO create tables
```
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;

CREATE TABLE region (
  R_REGIONKEY bigint NOT NULL,
  R_NAME varchar(25),
  R_COMMENT varchar(152))
diststyle all;

CREATE TABLE nation (
  N_NATIONKEY bigint NOT NULL,
  N_NAME varchar(25),
  N_REGIONKEY bigint,
  N_COMMENT varchar(152))
diststyle all;

create table customer (
  C_CUSTKEY bigint NOT NULL,
  C_NAME varchar(25),
  C_ADDRESS varchar(40),
  C_NATIONKEY bigint,
  C_PHONE varchar(15),
  C_ACCTBAL decimal(18,4),
  C_MKTSEGMENT varchar(10),
  C_COMMENT varchar(117))
diststyle all;

create table orders (
  O_ORDERKEY bigint NOT NULL,
  O_CUSTKEY bigint,
  O_ORDERSTATUS varchar(1),
  O_TOTALPRICE decimal(18,4),
  O_ORDERDATE Date,
  O_ORDERPRIORITY varchar(15),
  O_CLERK varchar(15),
  O_SHIPPRIORITY Integer,
  O_COMMENT varchar(79))
distkey (O_ORDERKEY)
sortkey (O_ORDERDATE);

create table part (
  P_PARTKEY bigint NOT NULL,
  P_NAME varchar(55),
  P_MFGR  varchar(25),
  P_BRAND varchar(10),
  P_TYPE varchar(25),
  P_SIZE integer,
  P_CONTAINER varchar(10),
  P_RETAILPRICE decimal(18,4),
  P_COMMENT varchar(23))
diststyle all;

create table supplier (
  S_SUPPKEY bigint NOT NULL,
  S_NAME varchar(25),
  S_ADDRESS varchar(40),
  S_NATIONKEY bigint,
  S_PHONE varchar(15),
  S_ACCTBAL decimal(18,4),
  S_COMMENT varchar(101))
diststyle all;

create table lineitem (
  L_ORDERKEY bigint NOT NULL,
  L_PARTKEY bigint,
  L_SUPPKEY bigint,
  L_LINENUMBER integer NOT NULL,
  L_QUANTITY decimal(18,4),
  L_EXTENDEDPRICE decimal(18,4),
  L_DISCOUNT decimal(18,4),
  L_TAX decimal(18,4),
  L_RETURNFLAG varchar(1),
  L_LINESTATUS varchar(1),
  L_SHIPDATE date,
  L_COMMITDATE date,
  L_RECEIPTDATE date,
  L_SHIPINSTRUCT varchar(25),
  L_SHIPMODE varchar(10),
  L_COMMENT varchar(44))
distkey (L_ORDERKEY)
sortkey (L_RECEIPTDATE);

create table partsupp (
  PS_PARTKEY bigint NOT NULL,
  PS_SUPPKEY bigint NOT NULL,
  PS_AVAILQTY integer,
  PS_SUPPLYCOST decimal(18,4),
  PS_COMMENT varchar(199))
diststyle even;
```

#### Copy the following statements to load data into 7 tables.

```
COPY region FROM 's3://redshift-immersionday-labs/data/region/region.tbl.lzo'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

copy customer from 's3://redshift-immersionday-labs/data/customer/customer.tbl.'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

copy orders from 's3://redshift-immersionday-labs/data/orders/orders.tbl.'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

copy part from 's3://redshift-immersionday-labs/data/part/part.tbl.'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

copy supplier from 's3://redshift-immersionday-labs/data/supplier/supplier.json' manifest
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;

copy lineitem from 's3://redshift-immersionday-labs/data/lineitem-part/'
iam_role default
region 'us-west-2' gzip delimiter '|' COMPUPDATE PRESET;

copy partsupp from 's3://redshift-immersionday-labs/data/partsupp/partsupp.tbl.'
iam_role default
region 'us-west-2' lzop delimiter '|' COMPUPDATE PRESET;
```

Navigate to AWS CloudFormation  and click on 'cfn' stack. Under 'Outputs' tab note down the S3Bucket. You’ll need this value in next step.

You can download the sample Nation.csv file onto your local machine using the below link.

Nation file - https://redshift-immersionday-labs.s3.us-west-2.amazonaws.com/data/nation/Nation.csv

Click on Load data 
-- select Load from local file 
-- Browse and select the Nation.csv file downloaded earlier 
-- Data conversion parameters 
-- Check Ignore header rows -- Next -- Table options select your serverless work group -- Database = dev -- Schema = public -- Table = nation -- Load data -- Notification of Loading data from a local file succeeded would be shown on top.


#### Load Validation

 --Number of rows= 5
select count(*) from region;

 --Number of rows= 25
select count(*) from nation;

 --Number of rows= 76,000,000
select count(*) from orders;

#### Troubleshooting Loads
To troubleshoot any data load issues, you can query SYS_LOAD_ERROR_DETAIL.

In addition, you can validate your data without actually loading the table. You can use the NOLOAD option with the COPY command to make sure that your data will load without any errors before running the actual data load. Notice that running COPY with the NOLOAD option is much faster than loading the data since it only parses the files.

Let’s try to load the CUSTOMER table with a different data file with mismatched columns.
```
COPY customer FROM 's3://redshift-immersionday-labs/data/nation/nation.tbl.'
iam_role default
region 'us-west-2' lzop delimiter '|' noload;
```
You will get the following error.
```
ERROR: Load into table 'customer' failed. Check 'sys_load_error_detail' system table for details.
```
Query the STL_LOAD_ERROR system table for details.
```
select * from SYS_LOAD_ERROR_DETAIL;
```
Notice that there is one row for the column c_nationkey with datatype int with an error message "Invalid digit, Value 'h', Pos 1" indicating that you are trying to load character into an integer column.

Automatic Table Maintenance - ANALYZE and VACUUM
Analyze:

When loading into an empty table, the COPY command by default collects statistics (ANALYZE). If you are loading a non-empty table using COPY command, in most cases, you don't need to explicitly run the ANALYZE command. Amazon Redshift monitors changes to your workload and automatically updates statistics in the background. To minimize impact to your system performance, automatic analyze runs during periods when workloads are light.

If you need to analyze the table immediately after load, you can still manually run ANALYZE command.

To run ANALYZE on orders table, copy the following command and run it.

1
ANALYZE orders;

Vacuum:

Vacuum Delete: When you perform delete on a table, the rows are marked for deletion(soft deletion), but are not removed. When you perform an update, the existing rows are marked for deletion(soft deletion) and updated rows are inserted as new rows. Amazon Redshift automatically runs a VACUUM DELETE operation in the background to reclaim disk space occupied by rows that were marked for deletion by UPDATE and DELETE operations, and compacts the table to free up the consumed space. To minimize impact to your system performance, automatic VACUUM DELETE runs during periods when workloads are light.

If you need to reclaim diskspace immediately after a large delete operation, for example after a large data load, then you can still manually run the VACUUM DELETE command. Lets see how VACUUM DELETE reclaims table space after delete operation.

First, capture tbl_rows(Total number of rows in the table. This value includes rows marked for deletion, but not yet vacuumed) and estimated_visible_rows(The estimated visible rows in the table. This value does not include rows marked for deletion) for the ORDERS table. Copy the following command and run it.

1
2
3
select "table", size, tbl_rows, estimated_visible_rows
from SVV_TABLE_INFO
where "table" = 'orders';

table	size	tbl_rows	estimated_visible_rows
orders	6100	76000000	76000000
Next, delete rows from the ORDERS table. Copy the following command and run it.

1
delete orders where o_orderdate between '1997-01-01' and '1998-01-01';

Next, capture the tbl_rows and estimated_visible_rows for ORDERS table after the deletion.

Copy the following command and run it. Notice that the tbl_rows value hasn't changed, after deletion. This is because rows are marked for soft deletion, but VACUUM DELETE is not yet run to reclaim space.

1
2
3
select "table", size, tbl_rows, estimated_visible_rows
from SVV_TABLE_INFO
where "table" = 'orders';

table	size	tbl_rows	estimated_visible_rows
orders	5972	76000000	64436860
Now, run the VACUUM DELETE command. Copy the following command and run it.

1
vacuum delete only orders;

Confirm that the VACUUM command reclaimed space by running the following query again and noting the tbl_rows value has changed.

1
2
3
select "table", size, tbl_rows, estimated_visible_rows
from SVV_TABLE_INFO
where "table" = 'orders';

table	size	tbl_rows	estimated_visible_rows
orders	5248	64436859	64436860
Vacuum Sort: When you define SORT KEYS  on your tables, Amazon Redshift automatically sorts data in the background to maintain table data in the order of its sort key. Having sort keys on frequently filtered columns makes block level pruning, which is already efficient in Amazon Redshift, more efficient.

Amazon Redshift keeps track of your scan queries to determine which sections of the table will benefit from sorting and it automatically runs VACUUM SORT to maintain sort key order. To minimize impact to your system performance, automatic VACUUM SORT runs during periods when workloads are light.

COPY command automatically sorts and loads the data in sort key order. As a result, if you are loading an empty table using COPY command, the data is already in sort key order. If you are loading a non-empty table using COPY command, you can optimize the loads by loading data in incremental sort key order because VACUUM SORT will not be needed when your load is already in sort key order

For example, orderdate is the sort key on orders table. If you always load data into orders table where orderdate is the current date, since current date is forward incrementing, data will always be loaded in incremental sortkey(orderdate) order. Hence, in that case VACUUM SORT will not be needed for orders table.

If you need to run VACUUM SORT, you can still manually run it as shown below. Copy the following command and run it.

```
vacuum sort only orders;

Vacuum recluster:
```
Use VACUUM recluster whenever possible for manual VACUUM operations. This is especially important for large objects with frequent ingestion and queries that access only the most recent data. Vacuum recluster only sorts the portions of the table that are unsorted and hence runs faster. Portions of the table that are already sorted are left intact. This command doesn't merge the newly sorted data with the sorted region. It also doesn't reclaim all space that is marked for deletion. In order to run vacuum recluster on orders, copy the following command and run it.

```
vacuum recluster orders;
```
Vacuum boost:

Boost runs the VACUUM command with additional compute resources, as they're available. With the BOOST option, VACUUM operates in one window and blocks concurrent deletes and updates for the duration of the VACUUM operation. Note that running vacuum with the BOOST option contends for system resources, which might affect performance of other queries. As a result, it is recommended to run the VACUUM BOOST when the load on the system is light, such as during maintenance operations. In order to run vacuum recluster on orders table in boost mode, copy the following command and run it.

```
vacuum recluster orders boost;
```
