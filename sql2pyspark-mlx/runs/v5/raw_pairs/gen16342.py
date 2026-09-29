from pyspark.sql import functions as F

with_dims = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .filter(F.col("d.balance") < 19000)
    .select("a.*", F.col("d.opened_date"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("amount") >= 4000, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("txn_type") == "FEE", 1)), F.lit(0)), 2).alias("amount_per_fee"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.max(F.when(F.col("merchant") == "7-Eleven", F.col("amount"))).alias("7_eleven_max_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.stddev_samp("amount").alias("amount_stddev_2"),
    )
    .filter(F.col("7_eleven_max_amount") >= 0)
)
