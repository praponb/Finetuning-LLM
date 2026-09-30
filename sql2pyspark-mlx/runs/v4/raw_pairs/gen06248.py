from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "attributes",
        F.size("attributes").alias("num_attributes"),
        F.col("attributes")["device"].alias("device"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.size("items").alias("num_items"),
    )
)

result = (
    parsed
    .groupBy(
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        F.when(F.col("num_items") >= 3, "LARGE").otherwise("SMALL").alias("num_items_tier"),
    )
    .agg(
        F.max("num_attributes").alias("max_num_attributes"),
        F.countDistinct("device").alias("distinct_devices"),
        F.round(F.avg("num_items"), 2).alias("avg_num_items"),
    )
)
