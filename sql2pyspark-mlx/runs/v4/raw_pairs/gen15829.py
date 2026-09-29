from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"), "element")
    .agg(
        F.count(F.when(F.col("element") == "VIP", 1)).alias("vip_count"),
        F.count(F.when(F.col("element") == "BUNDLE", 1)).alias("bundle_count"),
        F.min("created_at").alias("first_created_at"),
        F.count("*").alias("event_count"),
    )
)
