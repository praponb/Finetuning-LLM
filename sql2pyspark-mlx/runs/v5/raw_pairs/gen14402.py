from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") == 15246)
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
        F.count("sqft").alias("sqft_count"),
    )
    .filter(F.col("sqft_count") >= 4)
    .drop("sqft_count")
    .orderBy(F.col("total_sqft").desc())
)
