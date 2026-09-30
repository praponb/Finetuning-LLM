from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("order_id").orderBy(F.col("discount").desc(), "id")
w3 = Window.partitionBy("status", "order_id")
w4 = Window.partitionBy("status", "order_id").orderBy(F.col("status_sum_discount").asc_nulls_last(), "id")
w5 = Window.partitionBy("order_id").orderBy("discount_filled", "id")
w6 = Window.partitionBy("order_id").orderBy("discount_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy("status_sum_discount", "id").rowsBetween(-3, -1)
w8 = Window.partitionBy("order_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_payments
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.discount"))
)

valid = (
    joined
    .filter((F.col("paid_at") >= F.lit("2023-05-12").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "status",
        "discount",
        F.when(F.col("amount") >= 1540, "High").when(F.col("amount") >= 610, "Medium").otherwise("Low").alias("amount_tier"),
        F.trim("status").alias("clean_status"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.stddev_samp("discount").over(w).alias("order_id_stddev_samp_discount"),
        F.row_number().over(w2).alias("event_seq"),
        F.sum("discount").over(w3).alias("status_sum_discount"),
    )
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

base = (
    ranked
    .filter(F.col("status_sum_discount") == 2)
    .select(
        "id",
        "order_id",
        "discount",
        "amount_tier",
        "clean_status",
        "order_id_stddev_samp_discount",
        "event_seq",
        "status_sum_discount",
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
    )
)

with_windows_payments = (
    base
    .select(
        "id",
        "clean_status",
        "order_id_stddev_samp_discount",
        "event_seq",
        F.row_number().over(w5).alias("event_seq_2"),
        F.sum("order_id_stddev_samp_discount").over(w6).alias("running_order_id_stddev_samp_discount"),
        F.avg("event_seq").over(w7).alias("event_seq_avg_3_prior"),
        F.sum("order_id_stddev_samp_discount").over(w8).alias("running_order_id_stddev_samp_discount_2"),
    )
)

result = (
    with_windows_payments
    .groupBy(
        F.when(F.col("running_order_id_stddev_samp_discount_2") >= 2, "LARGE").otherwise("SMALL").alias("running_order_id_stddev_samp_discount_2_tier"),
    )
    .agg(
        F.percentile_approx("event_seq", 0.5).alias("event_seq_p50"),
        F.percentile_approx("order_id_stddev_samp_discount", 0.5).alias("order_id_stddev_samp_discount_p50"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("order_id_stddev_samp_discount_p50").desc_nulls_last())
)
