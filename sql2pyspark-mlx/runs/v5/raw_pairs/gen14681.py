from pyspark.sql import functions as F

result = (
    df_transactions
    .withColumn("quarter_start", F.date_trunc("QUARTER", "txn_time"))
    .withColumn("year_start", F.date_trunc("YEAR", "txn_time"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 4400, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["quarter_start", "year_start", "amount_tier"], ["quarter_start", "year_start"], []],
        "quarter_start",
        "year_start",
        "amount_tier",
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_rows"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount"))).alias("credit_total_amount"),
    )
    .orderBy(F.col("amount_p50").desc_nulls_first())
)
