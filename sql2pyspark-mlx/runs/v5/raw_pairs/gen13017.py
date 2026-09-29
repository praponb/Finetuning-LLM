from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id", F.date_trunc("QUARTER", "txn_time").alias("quarter_start"))
    .agg(
        F.count("*").alias("row_count"),
        F.max("txn_time").alias("last_seen"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
    )
    .filter(F.col("distinct_account_ids") > 1)
)
