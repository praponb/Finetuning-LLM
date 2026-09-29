from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["Central", "West"]) & (F.col("city") == "Chiang Mai"))
    .groupBy("region", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
    .filter(F.col("pct_chiang_mai") < 68)
)
