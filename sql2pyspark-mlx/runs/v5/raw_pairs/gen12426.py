from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("discount", "id")
w2 = Window.partitionBy("customer_id", "store_id").orderBy("shipping_fee", "id")
w_ordered_2 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("channel").orderBy("order_date", "id")
w5 = Window.partitionBy("channel").orderBy("amount", "id")
w6 = Window.partitionBy("channel").orderBy("order_date", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("channel").orderBy(F.col("row_seq").asc_nulls_last())
w8 = Window.partitionBy("channel")

metrics = (
    df_orders
    .select(
        "id",
        "order_date",
        "channel",
        "amount",
        "shipping_fee",
        F.row_number().over(w_ordered).alias("event_seq"),
        F.lag("amount", 1).over(w2).alias("prev_amount"),
        F.sum("shipping_fee").over(w_ordered_2).alias("cumulative_shipping_fee"),
        F.lead("discount", 2).over(w_hist).alias("next_discount"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "order_date",
        "amount",
        "cumulative_shipping_fee",
        "next_discount",
        F.avg("cumulative_shipping_fee").over(w6).alias("cumulative_shipping_fee_avg_3"),
        F.percent_rank().over(w7).alias("row_seq_percent_rank"),
        F.corr("shipping_fee", "amount").over(w8).alias("corr_shipping_fee_amount"),
    )
)

base = (
    with_windows
    .filter((F.col("order_date") >= F.lit("2023-03-27").cast("date")) & (F.col("cumulative_shipping_fee") > 197))
    .select(
        "*",
        F.round("amount", 2).alias("amount_rounded"),
        (F.col("cumulative_shipping_fee") * F.col("cumulative_shipping_fee_avg_3")).alias("cumulative_shipping_fee_times_cumulative_shipping_fee_avg_3"),
    )
)

filtered = (
    base
    .filter(F.col("order_date") > F.lit("2023-09-03").cast("date"))
    .select(
        "id",
        "amount",
        "next_discount",
        "cumulative_shipping_fee_avg_3",
        "row_seq_percent_rank",
        "corr_shipping_fee_amount",
        "cumulative_shipping_fee_times_cumulative_shipping_fee_avg_3",
        F.round("corr_shipping_fee_amount", 1).alias("corr_shipping_fee_amount_rounded"),
        (F.col("cumulative_shipping_fee_times_cumulative_shipping_fee_avg_3") * F.col("cumulative_shipping_fee_avg_3")).alias("cumulative_shipping_fee_times_cumulative_shipping_fee_avg_3_times_cumulative_shipping_fee_avg_3"),
    )
)

remaining_keys = (
    filtered
    .select("id")
    .exceptAll(
        df_orders
        .filter(F.col("amount").between(470, 860))
        .select("id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("cumulative_shipping_fee_avg_3") >= 224, "LARGE").otherwise("SMALL").alias("cumulative_shipping_fee_avg_3_tier"),
    )
    .agg(
        F.percentile_approx("corr_shipping_fee_amount_rounded", 0.9).alias("corr_shipping_fee_amount_rounded_p90"),
        F.round(F.avg("cumulative_shipping_fee_times_cumulative_shipping_fee_avg_3"), 2).alias("avg_cumulative_shipping_fee_times_cumulative_shipping_fee_avg_3"),
        F.percentile_approx("corr_shipping_fee_amount", 0.9).alias("corr_shipping_fee_amount_p90"),
    )
    .filter(F.col("corr_shipping_fee_amount_p90") < 1000000)
    .orderBy("avg_cumulative_shipping_fee_times_cumulative_shipping_fee_avg_3")
)
