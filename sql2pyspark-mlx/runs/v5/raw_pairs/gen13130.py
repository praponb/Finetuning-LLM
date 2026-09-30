from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(8375, 13304))
    .groupBy("city", "region")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "East", 1).otherwise(0))) / F.count("*")), 1).alias("pct_east"),
    )
    .filter(F.col("pct_east") >= 50)
    .select("city", "region", "pct_east")
)
