from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

result = (
    lines
    .groupBy("customer_id", "key")
    .agg(
        F.count(F.when(F.col("key") == "utm_source", 1)).alias("utm_source_rows"),
        F.countDistinct("key").alias("distinct_keys"),
    )
    .filter(F.col("distinct_keys") >= 1)
    .orderBy(F.col("key").asc_nulls_first())
)
