from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.orderBy(F.col("qty").asc_nulls_first())
w3 = Window.orderBy(F.col("price_count_30m").desc_nulls_last())
w_part = Window.orderBy(F.col("qty_dense_rank").desc_nulls_last())
w5 = Window.orderBy(F.col("price").desc_nulls_last())
w6 = Window.orderBy(F.col("qty_dense_rank_cume_dist").desc_nulls_last())
w7 = Window.orderBy(F.col("days_since_created_at").asc_nulls_first())
w8 = Window.orderBy(F.col("price").desc())

exploded = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
    )
)

metrics = (
    exploded
    .withColumn("price_count_30m", F.count("price").over(w))
    .withColumn("product_id_max_30m", F.max("product_id").over(w))
    .withColumn("qty_dense_rank", F.dense_rank().over(w2))
)

base = (
    metrics
    .filter(F.col("created_at") > F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "coupon_codes",
        "product_id",
        "price",
        "price_count_30m",
        "product_id_max_30m",
        "qty_dense_rank",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.cume_dist().over(w3).alias("price_count_30m_cume_dist"),
        F.cume_dist().over(w_part).alias("qty_dense_rank_cume_dist"),
        F.dense_rank().over(w5).alias("price_dense_rank"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("price").between(210, 220))
    .select(
        "id",
        "coupon_codes",
        "price",
        "price_count_30m",
        "product_id_max_30m",
        "qty_dense_rank",
        "days_since_created_at",
        "price_count_30m_cume_dist",
        "qty_dense_rank_cume_dist",
        F.greatest("days_since_created_at", "qty_dense_rank_cume_dist").alias("greatest_days_since_created_at_qty_dense_rank_cume_dist"),
        F.round((F.col("qty_dense_rank_cume_dist") * 0.9), 2).alias("adjusted_qty_dense_rank_cume_dist"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w6).alias("qty_dense_rank_cume_dist_dense_rank"),
        F.dense_rank().over(w7).alias("days_since_created_at_dense_rank"),
        F.cume_dist().over(w8).alias("price_cume_dist"),
    )
)

filtered = (
    scored
    .filter(F.array_contains("coupon_codes", "VIP"))
    .select(
        "price",
        "price_count_30m",
        "product_id_max_30m",
        "days_since_created_at",
        "price_count_30m_cume_dist",
        "greatest_days_since_created_at_qty_dense_rank_cume_dist",
        "adjusted_qty_dense_rank_cume_dist",
        "qty_dense_rank_cume_dist_dense_rank",
        "days_since_created_at_dense_rank",
        "price_cume_dist",
        (F.col("price") - F.col("price_count_30m_cume_dist")).alias("price_minus_price_count_30m_cume_dist"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("product_id_max_30m") >= 11, "HIGH").otherwise("LOW").alias("product_id_max_30m_tier"),
    )
    .agg(
        F.round(F.avg("price_minus_price_count_30m_cume_dist"), 2).alias("avg_price_minus_price_count_30m_cume_dist"),
        F.min("adjusted_qty_dense_rank_cume_dist").alias("min_adjusted_qty_dense_rank_cume_dist"),
        F.round(F.avg("price_cume_dist"), 2).alias("avg_price_cume_dist"),
        F.percentile_approx("days_since_created_at_dense_rank", [0.25, 0.5, 0.75]).alias("days_since_created_at_dense_rank_quartiles"),
        F.percentile_approx("product_id_max_30m", 0.95).alias("product_id_max_30m_p95"),
    )
    .filter(F.col("avg_price_cume_dist") >= 0)
    .orderBy(F.col("product_id_max_30m_tier").asc_nulls_last())
)
