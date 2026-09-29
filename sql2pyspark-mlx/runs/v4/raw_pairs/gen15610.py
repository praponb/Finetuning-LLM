from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("customer_id")
w3 = Window.orderBy(F.col("price_avg_30m").desc())
w4 = Window.partitionBy("customer_id").orderBy(F.col("price_avg_30m").desc())

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
    )
)

with_windows = (
    flattened
    .select(
        "*",
        F.avg("price").over(w).alias("price_avg_30m"),
        F.max("qty").over(w2).alias("customer_id_max_qty"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.count("qty").over(w2).alias("customer_id_count_qty"),
        F.min("price_avg_30m").over(w2).alias("customer_id_min_price_avg_30m"),
        F.dense_rank().over(w3).alias("price_avg_30m_dense_rank"),
        F.cume_dist().over(w4).alias("price_avg_30m_cume_dist"),
        F.round((F.col("customer_id_max_qty") / F.sum("customer_id_max_qty").over(w2)), 4).alias("customer_id_max_qty_share"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("customer_id_max_qty_share") >= 0.64, "PREMIUM").otherwise("STANDARD").alias("customer_id_max_qty_share_tier"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.sum("price_avg_30m").alias("total_price_avg_30m"),
        F.min("created_at").alias("first_created_at"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.percentile_approx("price_avg_30m_cume_dist", [0.25, 0.5, 0.75]).alias("price_avg_30m_cume_dist_quartiles"),
    )
    .orderBy(F.col("span_minutes").desc_nulls_first())
)
