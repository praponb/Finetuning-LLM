from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_trunc("week", "txn_time").alias("week_start"), "txn_type")
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.count_if(F.col("txn_type") == "REFUND").alias("refund_events"),
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
        F.max("txn_time").alias("last_seen"),
    )
)
