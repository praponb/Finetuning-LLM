from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.date_trunc("QUARTER", "paid_at").alias("quarter_start"))
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        F.max("paid_at").alias("ended_at"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count_if(F.col("method") == "card").alias("card_events"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
    )
    .filter(F.col("failed_count") < 1)
)
