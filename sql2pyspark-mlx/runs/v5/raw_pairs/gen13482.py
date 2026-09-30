from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "coupon_codes", F.explode_outer("attributes").alias("key", "v"))
)

result = (
    lines
    .groupBy("key")
    .agg(
        F.count("*").alias("row_count"),
        F.count_if(F.col("key") == "device").alias("device_events"),
    )
    .orderBy(F.col("device_events").desc_nulls_last())
)
