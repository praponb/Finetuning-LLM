from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("pos").desc_nulls_first())
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("customer_id", "city").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("code").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "line"))
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

flattened_carts = (
    flattened
    .select(
        "id",
        "customer_id",
        "created_at",
        "pos",
        "product_id",
        "qty",
        F.explode("coupon_codes").alias("code"),
    )
)

with_windows = (
    flattened_carts
    .select(
        "*",
        F.dense_rank().over(w).alias("pos_dense_rank"),
        F.max("qty").over(w2).alias("qty_max_30m"),
    )
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.city"), F.col("d.signup_date"))
)

metrics = (
    with_dims
    .withColumn("qty_max_30m_count_30m", F.count("qty_max_30m").over(w3))
    .withColumn("qty_max_30m_share", F.round((F.col("qty_max_30m") / F.sum("qty_max_30m").over(w4)), 4))
)

enriched = (
    metrics
    .select(
        "*",
        F.stddev_samp("qty").over(w4).alias("customer_id_stddev_samp_qty"),
        F.max("pos").over(w5).alias("pos_max_30m"),
        F.avg("qty_max_30m_count_30m").over(w6).alias("qty_max_30m_count_30m_avg_15m"),
    )
)

result = (
    enriched
    .withColumn("pos_tier", F.when(F.col("pos") >= 3, "PREMIUM").otherwise("STANDARD"))
    .withColumn("qty_max_30m_count_30m_tier", F.when(F.col("qty_max_30m_count_30m") >= 16, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["pos_tier", "qty_max_30m_count_30m_tier", "city"], ["pos_tier", "qty_max_30m_count_30m_tier"]],
        "pos_tier",
        "qty_max_30m_count_30m_tier",
        "city",
    )
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.max(F.when(F.col("code") == "SPRING10", F.col("qty_max_30m"))).alias("spring10_max_qty_max_30m"),
        F.sum("product_id").alias("total_product_id"),
    )
    .filter(F.col("distinct_cities") > 1)
)
