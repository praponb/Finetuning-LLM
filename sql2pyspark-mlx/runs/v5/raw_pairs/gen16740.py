from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("quarter", "opened_date").alias("quarter_start"))
    .agg(
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_count"),
        F.stddev_samp("sqft").alias("sqft_stddev_2"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
    .orderBy(F.col("sqft_stddev_2").desc_nulls_first())
)
