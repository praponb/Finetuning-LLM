from pyspark.sql import functions as F

src = (
    df_transactions
    .filter((F.col("account_id") == 24) & F.col("merchant").isin(["Lazada", "Amazon", "Starbucks"]))
    .withColumn("adjusted_amount", F.round((F.col("amount") * 1.15), 2))
)

summary = (
    src
    .groupBy(
        F.when(F.col("txn_type").isin(["DEBIT", "REFUND"]), "core").otherwise("other").alias("txn_type_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    summary
    .select("txn_type_group", "txn_type_values")
    .orderBy("txn_type_values", "txn_type_group")
    .limit(20)
)
