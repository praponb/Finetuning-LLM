from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
        F.array_contains("coupon_codes", "WELCOME5").alias("used_welcome5"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("unknown")).alias("gift_or_default"),
        F.size(F.filter("items", lambda x: x["qty"] >= 3)).alias("bulk_lines"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("bulk_lines") >= 3, "LARGE").otherwise("SMALL").alias("bulk_lines_tier"),
    )
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.round(F.avg("bulk_lines"), 2).alias("avg_bulk_lines"),
    )
)
