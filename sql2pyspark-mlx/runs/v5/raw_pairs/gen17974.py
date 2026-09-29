from pyspark.sql import functions as F

result = (
    df_stores
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "East", 1).otherwise(0))) / F.count("*")), 1).alias("pct_east"),
        F.max("sqft").alias("max_sqft"),
    )
    .orderBy("pct_east")
)
