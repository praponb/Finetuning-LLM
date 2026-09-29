from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "West") & (F.col("region") == "Central"))
    .withColumn("is_south", F.when(F.col("region") == "South", 1).otherwise(0))
    .withColumn("store_name_length", F.length("store_name"))
    .orderBy("region")
)
