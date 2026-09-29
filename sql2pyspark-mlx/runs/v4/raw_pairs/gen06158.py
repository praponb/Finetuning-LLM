from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

result = (
    lines
    .groupBy("customer_id")
    .agg(
        F.countDistinct("key").alias("distinct_keys"),
        F.countDistinct("key").alias("distinct_keys_2"),
    )
    .filter(F.col("distinct_keys_2") < 1)
    .orderBy(F.col("distinct_keys").desc_nulls_last())
)
