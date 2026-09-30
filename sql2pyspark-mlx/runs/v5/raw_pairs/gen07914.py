from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(-3, -1)
w2 = Window.orderBy(F.col("amount").desc())
w_grp = Window.partitionBy("method").orderBy("paid_at_period", "id")
w4 = Window.partitionBy("method").orderBy(F.col("amount_avg_3_prior").desc_nulls_first(), "id")
w5 = Window.partitionBy("method")
w6 = Window.orderBy("paid_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("method").orderBy("amount_percent_rank")
w8 = Window.orderBy("paid_at_period", "id").rowsBetween(-2, Window.currentRow)
w9 = Window.partitionBy("method").orderBy("paid_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("amount_tier").orderBy(F.col("cumulative_amount_percent_rank_p90").desc())

base = (
    df_payments
    .filter((F.col("method").eqNullSafe(F.lit("paypal"))) & (F.col("paid_at") >= F.lit("2023-07-30").cast("date")))
    .select("*", F.date_trunc("YEAR", "paid_at").alias("paid_at_period"))
)

valid = (
    base
    .filter((F.col("paid_at") > F.lit("2023-08-30 23:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .withColumn("method_group", F.when(F.col("method").isin(["promptpay", "bank_transfer"]), "Priority").otherwise("Other"))
)

with_windows = (
    valid
    .filter(F.col("order_id").isNull())
    .select(
        "*",
        F.avg("amount").over(w_running).alias("amount_avg_3_prior"),
        F.percent_rank().over(w2).alias("amount_percent_rank"),
        F.lag("amount", 2).over(w_grp).alias("prev_amount"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "method", "amount", "paid_at_period", "amount_percent_rank")
)

with_windows_payments = (
    latest
    .select(
        "id",
        "amount",
        F.avg("amount").over(w5).alias("method_avg_amount"),
        F.sum("amount_percent_rank").over(w6).alias("cumulative_amount_percent_rank"),
        F.dense_rank().over(w7).alias("amount_percent_rank_dense_rank"),
        F.avg("amount_percent_rank").over(w8).alias("amount_percent_rank_avg_2"),
        F.last("method", ignorenulls=True).over(w9).alias("last_known_method"),
    )
)

summary = (
    with_windows_payments
    .groupBy(
        F.when(F.col("amount") >= 1210, "LARGE").otherwise("SMALL").alias("amount_tier"),
        F.when(F.col("amount_percent_rank_avg_2") >= 1, "PREMIUM").otherwise("STANDARD").alias("amount_percent_rank_avg_2_tier"),
        F.when(F.col("cumulative_amount_percent_rank") >= 4, "PREMIUM").otherwise("STANDARD").alias("cumulative_amount_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("cumulative_amount_percent_rank", 0.9).alias("cumulative_amount_percent_rank_p90"),
        F.min("cumulative_amount_percent_rank").alias("min_cumulative_amount_percent_rank"),
        F.sum("cumulative_amount_percent_rank").alias("total_cumulative_amount_percent_rank"),
        F.count("*").alias("cnt"),
        F.max("amount_percent_rank_avg_2").alias("max_amount_percent_rank_avg_2"),
    )
    .withColumn("cumulative_amount_percent_rank_p90_rank", F.rank().over(w10))
)

result = (
    summary
    .select(
        "amount_tier",
        "amount_percent_rank_avg_2_tier",
        "cumulative_amount_percent_rank_tier",
        "cumulative_amount_percent_rank_p90",
        "min_cumulative_amount_percent_rank",
        "total_cumulative_amount_percent_rank",
        "max_amount_percent_rank_avg_2",
        "cumulative_amount_percent_rank_p90_rank",
    )
    .orderBy("amount_tier")
)
