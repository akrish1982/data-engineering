import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Standalone PySpark").getOrCreate()
print(spark.version)


# from pyspark.sql import SparkSession

# # Create the SparkSession with the required configurations
# spark = SparkSession.builder \
#     .appName("PySpark with Iceberg") \
#     .config("spark.jars.packages", 
#             "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
#             "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3") \
#     .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
#     .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
#     .config("spark.sql.catalog.s3tablesbucket.warehouse", 
#             "arn:aws:s3tables:us-east-1:131578276461:bucket/mys3testtablebucket") \
#     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
#     .getOrCreate()

# # Verify the Spark session is active
# print(spark.version)