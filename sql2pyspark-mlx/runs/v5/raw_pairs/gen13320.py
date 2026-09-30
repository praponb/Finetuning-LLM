from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("shipping_fee_whole").desc_nulls_first())
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("order_id").orderBy(F.col("shipping_fee").desc_nulls_last())
w_hist = Window.partitionBy("order_id").orderBy(F.col("shipping_fee_whole_rank").asc_nulls_first())

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("Kerry"))))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("N/A")))
)

kept_keys = (
    base
    .select("status", "id")
    .subtract(
        df_shipments
        .filter(F.col("status") != "IN_TRANSIT")
        .select("status", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id").eqNullSafe(F.col("d.id")), "inner")
    .filter(F.col("d.shipping_fee") >= 23)
    .select(
        "s.id",
        "s.order_id",
        "s.shipped_date",
        "s.cost",
        "s.status",
        "s.carrier_filled",
        F.col("d.discount"),
        F.col("d.shipping_fee"),
    )
)

filtered = (
    joined
    .filter(F.col("order_id").isNotNull() & (F.col("shipping_fee") <= 12))
    .withColumn("shipping_fee_whole", F.floor("shipping_fee"))
)

metrics = (
    filtered
    .select(
        "*",
        F.rank().over(w).alias("shipping_fee_whole_rank"),
        F.stddev_samp("shipping_fee").over(w2).alias("order_id_stddev_samp_shipping_fee"),
        F.max("shipping_fee").over(w3).alias("status_max_shipping_fee"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w4).alias("shipping_fee_dense_rank"),
        F.dense_rank().over(w_hist).alias("shipping_fee_whole_rank_dense_rank"),
        F.stddev_samp("shipping_fee_whole").over(w2).alias("order_id_stddev_samp_shipping_fee_whole"),
        F.stddev_samp("order_id_stddev_samp_shipping_fee").over(w2).alias("order_id_stddev_samp_order_id_stddev_samp_shipping_fee"),
    )
)

filtered_shipments = (
    scored
    .filter((F.col("shipped_date") >= F.lit("2023-04-06").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "shipping_fee",
        "shipping_fee_whole",
        "order_id_stddev_samp_shipping_fee",
        "status_max_shipping_fee",
        "shipping_fee_dense_rank",
        "shipping_fee_whole_rank_dense_rank",
        "order_id_stddev_samp_shipping_fee_whole",
        F.coalesce("shipping_fee_whole", F.lit(0)).alias("shipping_fee_whole_filled"),
    )
)

totals = (
    filtered_shipments
    .groupBy(
        F.when(F.col("order_id_stddev_samp_shipping_fee_whole") >= 68, "LARGE").otherwise("SMALL").alias("order_id_stddev_samp_shipping_fee_whole_tier"),
    )
    .agg(
        F.percentile_approx("shipping_fee_dense_rank", [0.25, 0.5, 0.75]).alias("shipping_fee_dense_rank_quartiles"),
        F.avg("shipping_fee_whole").alias("avg_shipping_fee_whole"),
        F.round(F.avg("order_id_stddev_samp_shipping_fee_whole"), 2).alias("avg_order_id_stddev_samp_shipping_fee_whole"),
        F.percentile_approx("shipping_fee_whole_filled", [0.25, 0.5, 0.75]).alias("shipping_fee_whole_filled_quartiles"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)

result = (
    totals
    .select(
        "order_id_stddev_samp_shipping_fee_whole_tier",
        "shipping_fee_dense_rank_quartiles",
        "avg_shipping_fee_whole",
        "shipping_fee_whole_filled_quartiles",
    )
    .orderBy(
        F.col("order_id_stddev_samp_shipping_fee_whole_tier").desc_nulls_last(),
        "avg_shipping_fee_whole",
    )
)
