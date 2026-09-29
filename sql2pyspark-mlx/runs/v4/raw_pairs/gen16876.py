from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.hour("created_at").between(10, 16))
    .select(
        "id",
        "created_at",
        "ship_to",
        F.array_contains("coupon_codes", "BUNDLE").alias("used_bundle"),
        F.size("items").alias("num_items"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("direct")).alias("utm_source_or_default"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    unpacked
    .rollup(
        F.when(F.col("num_items") >= 3, "HIGH").otherwise("LOW").alias("num_items_tier"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        F.countDistinct("utm_source_or_default").alias("distinct_utm_source_or_defaults"),
        F.round(F.avg("num_items"), 2).alias("avg_num_items"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
    )
    .filter(F.col("distinct_utm_source_or_defaults") >= 1)
    .orderBy("num_items_tier")
)
