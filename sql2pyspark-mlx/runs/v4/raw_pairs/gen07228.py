from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_trunc("YEAR", "txn_time").alias("year_start"))
    .agg(
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
    )
    .filter(F.col("fee_events") < 1)
    .orderBy(F.col("debit_amount").desc_nulls_last())
)
