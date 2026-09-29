from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.sum("sqft").alias("total_sqft"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
    )
)
