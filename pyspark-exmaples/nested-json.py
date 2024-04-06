import pyspark.sql.types as T
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Pyspark nested json").config("spark.memory.offHeap.enabled","true").config("spark.memory.offHeap.size","10g").getOrCreate()
rows = [
    {"id": 1, "typeId": 1, "items":[
        {"itemType": 1,"flag": False,"event": None},
        {"itemType": 3,"flag": True,"event":[{"info1": ""},{"info1": ""}]},
        {"itemType": 3,"flag": True,"event":[{"info1": ""},{"info1": ""}]},
    ]},
    {"id": 2, "typeId": 2, "items":None},
    {"id": 3, "typeId": 1, "items":[
        {"itemType": 1,"flag": False,"event": None},
        {"itemType": 6,"flag": False,"event":[{"info1": ""}]},
        {"itemType": 6,"flag": False,"event":None},
    ]},
    {"id": 4, "typeId": 2, "items":[
        {"itemType": 1,"flag": True,"event":[{"info1": ""}]},
    ]},
    {"id": 5, "typeId": 3, "items":None},
]

schema = T.StructType([
   T.StructField("id", T.IntegerType(), False),
   T.StructField("typeId", T.IntegerType()),
   T.StructField("items", T.ArrayType(T.StructType([
           T.StructField("itemType", T.IntegerType()),
           T.StructField("flag", T.BooleanType()),
           T.StructField("event", T.ArrayType(T.StructType([
                   T.StructField("info1", T.StringType()),
           ]))),
       ])), True),
])

df = spark.createDataFrame(rows, schema)
df.printSchema()

from pyspark.sql import functions as F

# Function to count items based on a condition
def count_items_condition(col, condition):
    return F.size(F.expr(f"filter({col}, item -> {condition})"))

# Function to sum the size of "event" arrays in "items"
def sum_events_size(col):
    return F.expr(f"aggregate({col}, 0, (acc, item) -> acc + size(item.event))")

# Add calculations as new columns
df = df.withColumn("total_requests", F.lit(1)) \
       .withColumn("total_requests_with_item", F.when(F.size("items") > 0, 1).otherwise(0)) \
       .withColumn("total_requests_with_item_flag_true", count_items_condition("items", "item.flag")) \
       .withColumn("total_items", F.size("items")) \
       .withColumn("total_flagged_items", count_items_condition("items", "item.flag")) \
       .withColumn("total_events_on_items", sum_events_size("items"))

# Group by typeId and sum the calculations
result = df.groupBy("typeId") \
           .agg(
               F.sum("total_requests").alias("total_requests"),
               F.sum("total_requests_with_item").alias("total_requests_with_item"),
               F.sum("total_requests_with_item_flag_true").alias("total_requests_with_item_flag_true"),
               F.sum("total_items").alias("total_items"),
               F.sum("total_flagged_items").alias("total_flagged_items"),
               F.sum("total_events_on_items").alias("total_events_on_items")
           )

result.show()
