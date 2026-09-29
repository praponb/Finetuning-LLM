from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy("amount")
w3 = Window.orderBy(F.col("amount").asc_nulls_first())
w4 = Window.orderBy(F.col("shipping_fee_rounded_filled").desc())
w5 = Window.orderBy(F.col("customer_id_avg_shipping_fee_rounded_filled").desc())

base = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-08-11").cast("date"))
    .select(
        "*",
        F.round("shipping_fee", 1).alias("shipping_fee_rounded"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

remaining_keys = (
    base
    .select("customer_id", "status")
    .subtract(
        df_orders
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id", "status")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("order_date") > F.lit("2024-01-13").cast("date"))
    .select(
        "customer_id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        "resolved_channel",
        F.coalesce("shipping_fee_rounded", F.lit(0)).alias("shipping_fee_rounded_filled"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

cleaned = (
    filtered
    .filter(F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        "order_date",
        "amount",
        "resolved_channel",
        "shipping_fee_rounded_filled",
        F.last_day("order_date").alias("order_date_month_end"),
        F.when(F.col("shipping_fee_rounded_filled") >= 89, "High").when(F.col("shipping_fee_rounded_filled") >= 28, "Medium").otherwise("Low").alias("shipping_fee_rounded_filled_band"),
    )
)

scored = (
    cleaned
    .withColumn("customer_id_avg_amount", F.avg("amount").over(w))
    .withColumn("amount_percent_rank", F.percent_rank().over(w2))
    .withColumn("amount_dense_rank", F.dense_rank().over(w3))
    .withColumn("customer_id_avg_shipping_fee_rounded_filled", F.avg("shipping_fee_rounded_filled").over(w))
    .withColumn("shipping_fee_rounded_filled_dense_rank", F.dense_rank().over(w4))
)

enriched = (
    scored
    .withColumn("customer_id_sum_amount", F.sum("amount").over(w))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("customer_id_avg_shipping_fee_rounded_filled_dense_rank", F.dense_rank().over(w5))
)

with_dims = (
    enriched
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.signup_date"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("shipping_fee_rounded_filled_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("shipping_fee_rounded_filled_dense_rank_tier"),
    )
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.round(F.avg("customer_id_avg_shipping_fee_rounded_filled_dense_rank"), 2).alias("avg_customer_id_avg_shipping_fee_rounded_filled_dense_rank"),
        F.percentile_approx("shipping_fee_rounded_filled_dense_rank", 0.5).alias("shipping_fee_rounded_filled_dense_rank_p50"),
    )
    .filter(F.col("avg_customer_id_avg_shipping_fee_rounded_filled_dense_rank") > 0)
    .orderBy(F.col("avg_customer_id_avg_shipping_fee_rounded_filled_dense_rank").desc())
)
