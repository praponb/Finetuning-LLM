from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy(F.col("paid_at").asc_nulls_first(), "id")
w_hist = Window.partitionBy("order_id", "status").orderBy("amount_per_rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("amount_per_rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("method").orderBy("running_avg_amount", "id")
w_ordered = Window.partitionBy("method").orderBy("running_count_amount", "id")
w6 = Window.partitionBy("method")

ranked = (
    df_payments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

valid = (
    ranked
    .filter((F.col("paid_at") > F.lit("2023-08-19 03:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "rank_in_group",
        F.round((F.col("amount") / F.col("rank_in_group")), 2).alias("amount_per_rank_in_group"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "amount_per_rank_in_group",
        F.avg("amount").over(w_hist).alias("running_avg_amount"),
        F.min("amount").over(w_hist).alias("running_min_amount"),
        F.count("amount").over(w_hist).alias("running_count_amount"),
        F.sum("amount").over(w_hist).alias("running_sum_amount"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "method",
        "amount_per_rank_in_group",
        "running_avg_amount",
        "running_sum_amount",
        F.sum("running_avg_amount").over(w3).alias("cumulative_running_avg_amount"),
        F.lag("amount", 1).over(w4).alias("prev_amount"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.round((F.col("running_avg_amount") / F.sum("running_avg_amount").over(w6)), 4).alias("running_avg_amount_share"),
    )
)

summary = (
    enriched
    .groupBy(
        F.when(F.col("prev_amount") >= 1340, "HIGH").otherwise("LOW").alias("prev_amount_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.when(F.col("method") == "bank_transfer", F.col("running_avg_amount_share"))).alias("bank_transfer_avg_running_avg_amount_share"),
        F.round(F.sum("running_avg_amount_share") / F.nullif(F.count(F.when(F.col("method") == "bank_transfer", 1)), F.lit(0)), 2).alias("running_avg_amount_share_per_bank_transfer"),
    )
)

result = summary.select("prev_amount_tier", "bank_transfer_avg_running_avg_amount_share")
