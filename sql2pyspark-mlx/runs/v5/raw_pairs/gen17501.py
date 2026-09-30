from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

result = (
    exploded
    .rollup(F.date_trunc("day", "created_at").alias("activity_day"), "key")
    .agg(
        F.countDistinct("key").alias("distinct_keys"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.countDistinct("key").alias("distinct_keys_2"),
        F.countDistinct("key").alias("distinct_keys_3"),
    )
    .orderBy(F.col("activity_day").desc())
)
