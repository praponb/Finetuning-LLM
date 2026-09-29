from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("price").asc_nulls_last())
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

flattened_carts = (
    flattened
    .select(
        "id",
        "created_at",
        "product_id",
        "qty",
        "price",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

valid = (
    flattened_carts
    .filter(F.col("tag").isNotNull() & (F.col("price") > 80))
    .select(
        "*",
        F.substring("tag", 1, 2).alias("tag_prefix"),
        F.coalesce("tag", F.lit("Unknown")).alias("tag_filled"),
    )
)

base = (
    valid
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & (F.col("product_id") != 4))
    .select(
        "created_at",
        "product_id",
        "price",
        "tag_prefix",
        "tag_filled",
        (F.col("qty") + F.col("price")).alias("qty_plus_price"),
        F.date_format("created_at", "MMM yyyy").alias("created_at_label"),
    )
)

enriched = (
    base
    .select(
        "product_id",
        "price",
        "tag_prefix",
        "tag_filled",
        "created_at_label",
        F.percent_rank().over(w_running).alias("price_percent_rank"),
        F.max("price").over(w2).alias("price_max_1h"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("price") >= 182, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
        "product_id",
    )
    .agg(
        F.percentile_approx("price_max_1h", 0.9).alias("price_max_1h_p90"),
        F.round(F.avg("price_max_1h"), 2).alias("avg_price_max_1h"),
    )
)
