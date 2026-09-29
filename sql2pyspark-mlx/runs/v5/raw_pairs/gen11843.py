from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date_period", "id")
w2 = Window.partitionBy("store_id").orderBy("order_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("customer_id", "store_id")
w5 = Window.partitionBy("store_id").orderBy(F.col("order_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

filtered = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-03-30"))) & (F.col("channel").eqNullSafe(F.lit("web"))))
    .withColumn("order_date_period", F.date_trunc("year", "order_date"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("NONE")))
)

valid = (
    filtered
    .filter(F.col("shipping_fee") >= 20)
    .select(
        "*",
        F.ceil("discount").alias("discount_whole"),
        F.hour("order_date_period").alias("order_date_period_hour"),
    )
)

with_gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("order_date_period").over(w).isNull() | (F.col("order_date_period") > F.lag("order_date_period").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "store_id",
        "amount",
        "shipping_fee",
        "order_date_period",
        "discount_whole",
        "order_date_period_hour",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

kept_keys = (
    sessionized
    .select("id")
    .intersectAll(
        df_orders
        .filter(F.col("customer_id").isin([18, 19]))
        .select("id")
    )
)

kept_rows = (
    sessionized
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "*",
        F.corr("amount", "shipping_fee").over(w3).alias("corr_amount_shipping_fee"),
        F.max("order_date_period_hour").over(w4).alias("customer_id_max_order_date_period_hour"),
        F.count("discount_whole").over(w5).alias("discount_whole_count_90d"),
    )
)

result = (
    with_windows
    .withColumn("burst_id_tier", F.when(F.col("burst_id") >= 8, "LARGE").otherwise("SMALL"))
    .rollup("burst_id_tier", "store_id")
    .agg(
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.min("order_date_period").alias("first_order_date_period"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.round(F.avg("discount_whole_count_90d"), 2).alias("avg_discount_whole_count_90d"),
        F.max("order_date_period").alias("last_seen"),
        ((F.unix_timestamp(F.max("order_date_period")) - F.unix_timestamp(F.min("order_date_period"))) / 60).alias("duration_min"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("burst_id_tier").desc_nulls_first())
)
