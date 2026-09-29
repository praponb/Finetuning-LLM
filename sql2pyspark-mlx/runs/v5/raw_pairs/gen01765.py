from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.partitionBy("customer_id").orderBy(F.col("is_vip").asc_nulls_last())
w3 = Window.partitionBy("customer_id")
w_running = Window.partitionBy("customer_id").orderBy(F.col("is_vip_cume_dist").desc())

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

lines_carts = (
    lines
    .select(
        "id",
        "customer_id",
        "created_at",
        "attr_key",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

base = (
    lines_carts
    .filter(F.col("attr_key") != "weight_kg")
    .select(
        "*",
        F.when(F.col("code") == "VIP", 1).otherwise(0).alias("is_vip"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled"),
    )
)

enriched = (
    base
    .filter(~F.col("attr_key").isin(["device", "weight_kg"]))
    .select(
        "customer_id",
        "attr_key",
        "is_vip",
        F.min("is_vip").over(w).alias("attr_key_min_is_vip"),
        F.cume_dist().over(w2).alias("is_vip_cume_dist"),
        F.max("is_vip").over(w3).alias("customer_id_max_is_vip"),
    )
)

scored = enriched.withColumn("is_vip_cume_dist_cume_dist", F.cume_dist().over(w_running))

cleaned = (
    scored
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("device"))))
    .select(
        "customer_id",
        "attr_key",
        "is_vip",
        "is_vip_cume_dist",
        "is_vip_cume_dist_cume_dist",
        (F.col("is_vip_cume_dist") + F.col("customer_id_max_is_vip")).alias("is_vip_cume_dist_plus_customer_id_max_is_vip"),
    )
)

result = (
    cleaned
    .groupBy(
        "attr_key",
        "customer_id",
        F.when(F.col("is_vip_cume_dist_cume_dist") >= 0.63, "LARGE").otherwise("SMALL").alias("is_vip_cume_dist_cume_dist_tier"),
    )
    .agg(
        F.count_if(F.col("attr_key") == "utm_source").alias("utm_source_events"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.round(F.sum("is_vip_cume_dist_cume_dist") / F.nullif(F.count(F.when(F.col("attr_key") == "gift", 1)), F.lit(0)), 2).alias("is_vip_cume_dist_cume_dist_per_gift"),
        F.count(F.when(F.col("attr_key") == "utm_source", 1)).alias("utm_source_count"),
    )
)
