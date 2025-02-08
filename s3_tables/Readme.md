
```
spark-shell \
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3 \
--conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog \
--conf spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:us-east-1:131578276461:bucket/mys3testtablebucket \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

then create a table:

```
spark.sql( 
" CREATE TABLE IF NOT EXISTS mys3testtablebucket.stocks.`example_table` ( 
    id INT, 
    name STRING, 
    value INT 
) 
USING iceberg "
)
```

using AWS CLI, we can create a namspace in the table:

```
aws s3tables create-namespace \
    --table-bucket-arn arn:aws:s3tables:us-east-1:131578276461:bucket/mys3testtablebucket \ 
    --namespace test_namespace
```

using AWS CLI, we can create s3 tables:

```
aws s3tables create-table \
    --table-bucket-arn arn:aws:s3tables:us-east-1:131578276461:bucket/mys3testtablebucket \
    --namespace test_namespace \
    --name example_table --format ICEBERG
```