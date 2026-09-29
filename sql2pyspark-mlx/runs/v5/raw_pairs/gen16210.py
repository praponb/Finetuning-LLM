from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("order_id", "status").orderBy("paid_at", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-3, -1)

metrics = (
    df_payments
    .filter(F.col("paid_at") < "2025-01-01")
    .select(
        "*",
        F.percent_rank().over(w).alias("amount_percent_rank"),
        F.avg("amount").over(w2).alias("amount_avg_5_prior"),
        F.avg("amount").over(w3).alias("amount_avg_3_prior"),
    )
)

result = (
    metrics
    .rollup(
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
    )
    .agg(
        F.round(F.sum("amount_avg_3_prior") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("amount_avg_3_prior_per_failed"),
        F.avg(F.when(F.col("method") == "bank_transfer", F.col("amount_percent_rank"))).alias("bank_transfer_avg_amount_percent_rank"),
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount_avg_3_prior")).otherwise(0)).alias("success_amount_avg_3_prior"),
    )
    .orderBy("quarter_start")
)
