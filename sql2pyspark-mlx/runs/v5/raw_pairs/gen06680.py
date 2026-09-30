from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 4)
    .groupBy(
        F.month("txn_time").alias("txn_month"),
        F.when(F.col("txn_type").isin(["DEBIT", "CREDIT"]), "tier_1").otherwise("other").alias("txn_type_group"),
    )
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Grab", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grab"),
    )
    .filter(F.col("pct_grab") < 64)
    .orderBy(F.col("amount_range").desc())
)
