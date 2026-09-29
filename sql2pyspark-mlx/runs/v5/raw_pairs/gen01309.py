from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Tokyo", "Berlin", "Singapore"]) & (F.col("region") == "West"))
    .withColumn("is_east", F.when(F.col("region") == "East", 1).otherwise(0))
    .withColumn("store_name_slug", F.regexp_replace("store_name", " ", "-"))
)
