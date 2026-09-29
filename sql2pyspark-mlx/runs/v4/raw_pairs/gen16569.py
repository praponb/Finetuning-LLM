from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value", "customer_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("pos").desc_nulls_first())
w3 = Window.partitionBy("value")
w4 = Window.partitionBy("promo", "value").orderBy("pos")
w5 = Window.partitionBy("promo")
w6 = Window.partitionBy("value").orderBy(F.col("value_max_qty").desc_nulls_first())

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.promo"),
    )
)

lines = (
    flattened
    .select(
        "id",
        "customer_id",
        "created_at",
        "pos",
        "qty",
        "promo",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.max("pos").over(w).alias("value_max_pos"),
        F.rank().over(w2).alias("pos_rank"),
        F.max("qty").over(w3).alias("value_max_qty"),
        F.cume_dist().over(w4).alias("pos_cume_dist"),
        F.stddev_samp("pos").over(w5).alias("promo_stddev_samp_pos"),
    )
)

valid = (
    with_windows
    .filter((F.col("created_at") >= F.lit("2025-03-02 14:00:00").cast("timestamp")) & F.col("value").isNotNull() & (F.col("promo") == "WELCOME5"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "pos",
        "qty",
        "promo",
        "value",
        "value_max_pos",
        "pos_rank",
        "value_max_qty",
        (F.col("value_max_pos") + F.col("pos")).alias("value_max_pos_plus_pos"),
        F.coalesce("value", F.lit("N/A")).alias("value_filled"),
    )
)

metrics = (
    valid
    .withColumn("value_max_qty_dense_rank", F.dense_rank().over(w6))
    .withColumn("promo_max_qty", F.max("qty").over(w5))
)

result = (
    metrics
    .cube(
        F.date_trunc("day", "created_at").alias("activity_day"),
        F.when(F.col("pos") >= 3, "LARGE").otherwise("SMALL").alias("pos_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("value") == "BUNDLE", F.col("pos_rank")).otherwise(0)).alias("bundle_pos_rank"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("promo") == "VIP", F.col("promo_max_qty"))).alias("vip_total_promo_max_qty"),
    )
    .filter(F.col("bundle_pos_rank") >= 0)
    .orderBy(F.col("event_count").desc())
)
