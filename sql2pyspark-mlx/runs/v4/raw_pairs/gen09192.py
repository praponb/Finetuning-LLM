from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("discount_tier").orderBy(F.col("shipping_fee_per_pending").desc())

base = (
    df_orders
    .filter(F.col("order_date") >= F.to_date(F.lit("2024-02-05")))
    .select("*", (F.col("discount") * F.col("amount")).alias("discount_times_amount"))
)

valid = (
    base
    .filter(F.col("customer_id").isNull())
    .select(
        "*",
        F.length("status").alias("status_length"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

matched_keys = (
    valid
    .select("id")
    .intersect(
        df_orders
        .filter(F.col("discount").isNotNull())
        .select("id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.delivered_date") >= F.col("s.order_date")), "left_semi")
)

eligible_orders = (
    eligible
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.status").isin(["RETURNED", "IN_TRANSIT"])), "left_anti")
)

summary = (
    eligible_orders
    .groupBy(
        F.when(F.col("discount") >= 0.31, "LARGE").otherwise("SMALL").alias("discount_tier"),
        F.when(F.col("amount") >= 1360, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("status") == "PENDING", 1)), F.lit(0)), 2).alias("shipping_fee_per_pending"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.avg(F.when(F.col("status") == "CANCELLED", F.col("amount"))).alias("cancelled_avg_amount"),
        F.count_if(F.col("status") == "COMPLETED").alias("completed_events"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("status_length")).otherwise(0)).alias("completed_status_length"),
    )
    .withColumn("shipping_fee_per_pending_dense_rank", F.dense_rank().over(w))
)

result = (
    summary
    .select(
        "discount_tier",
        "amount_tier",
        "discount_quartiles",
        "cancelled_avg_amount",
        "row_count",
        "completed_status_length",
        "shipping_fee_per_pending_dense_rank",
    )
    .orderBy(F.col("cancelled_avg_amount").desc_nulls_last())
)
