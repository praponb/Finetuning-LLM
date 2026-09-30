from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~((F.lower(F.trim("store_name")) == "downtown express") & (F.col("region").isin(["East", "West", "South"]) | F.col("region").isin(["Central", "West", "East"]))))
    .groupBy("city", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.count("*").alias("cnt"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy("min_opened_date", F.col("city").desc())
)
