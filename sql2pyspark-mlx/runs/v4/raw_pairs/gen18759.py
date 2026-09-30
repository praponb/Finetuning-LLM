from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.col("created_at") < "2025-07-01")
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.coalesce(F.col("attributes").getItem("device"), F.lit("unknown")).alias("device_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy("customer_id")
    .agg(
        F.max("created_at").alias("ended_at"),
        F.min("created_at").alias("started_at"),
        F.max("created_at").alias("last_seen"),
        F.countDistinct("device_or_default").alias("distinct_device_or_defaults"),
    )
    .orderBy(F.col("customer_id").desc_nulls_first())
)
