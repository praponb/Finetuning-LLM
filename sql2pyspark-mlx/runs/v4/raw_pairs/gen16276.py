from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["South", "East", "West"]))
    .withColumn("clean_region", F.trim("region"))
    .withColumn("is_north", F.when(F.col("region") == "North", 1).otherwise(0))
)
