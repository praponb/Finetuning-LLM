from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc())
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last())
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("segment", "country")
w5 = Window.partitionBy("country")

kept_keys = (
    df_orders
    .select("status", "channel")
    .subtract(
        df_orders
        .filter(F.col("store_id").isin([3, 6]))
        .select("status", "channel")
    )
)

reconciled = (
    df_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "customer_id",
        "order_date",
        "shipping_fee",
        F.dense_rank().over(w).alias("shipping_fee_dense_rank"),
        F.cume_dist().over(w2).alias("amount_cume_dist"),
        F.corr("shipping_fee", "amount").over(w3).alias("corr_shipping_fee_amount"),
    )
)

enriched = (
    with_windows
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "a.customer_id",
        "a.order_date",
        "a.shipping_fee",
        "a.shipping_fee_dense_rank",
        "a.amount_cume_dist",
        "a.corr_shipping_fee_amount",
        F.col("p.name"),
        F.col("p.country"),
        F.col("p.segment"),
    )
)

enriched_orders = (
    enriched
    .select(
        "order_date",
        "shipping_fee",
        "corr_shipping_fee_amount",
        "segment",
        F.count("shipping_fee_dense_rank").over(w4).alias("segment_count_shipping_fee_dense_rank"),
        F.sum("amount_cume_dist").over(w5).alias("country_sum_amount_cume_dist"),
    )
)

cleaned = (
    enriched_orders
    .filter((F.col("order_date") >= F.lit("2023-08-22").cast("date")) & (F.col("country_sum_amount_cume_dist") < 2))
    .select(
        "order_date",
        "shipping_fee",
        "corr_shipping_fee_amount",
        "segment",
        "country_sum_amount_cume_dist",
        F.round((F.col("shipping_fee") * 1.15), 2).alias("adjusted_shipping_fee"),
        (F.col("shipping_fee") + F.col("segment_count_shipping_fee_dense_rank")).alias("shipping_fee_plus_segment_count_shipping_fee_dense_rank"),
        F.coalesce("segment", F.lit("OTHER")).alias("resolved_segment"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("WEEK", "order_date").alias("week_start"))
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.percentile_approx("shipping_fee_plus_segment_count_shipping_fee_dense_rank", [0.25, 0.5, 0.75]).alias("shipping_fee_plus_segment_count_shipping_fee_dense_rank_quartiles"),
        F.round(F.sum("shipping_fee_plus_segment_count_shipping_fee_dense_rank") / F.nullif(F.count(F.when(F.col("segment") == "Corporate", 1)), F.lit(0)), 2).alias("shipping_fee_plus_segment_count_shipping_fee_dense_rank_per_corporate"),
        F.avg("adjusted_shipping_fee").alias("avg_adjusted_shipping_fee"),
        F.percentile_approx("shipping_fee", 0.5).alias("shipping_fee_p50"),
    )
    .filter(F.col("avg_adjusted_shipping_fee") > 0)
)
