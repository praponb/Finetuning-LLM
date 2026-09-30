from pyspark.sql import functions as F

dim_totals = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.status").alias("status"), F.col("e.txn_type").alias("txn_type"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("e.amount")).alias("total_amount"),
    )
)

result = (
    dim_totals
    .select(
        "status",
        "txn_type",
        "num_rows",
        "total_amount",
        F.when(F.col("txn_type").rlike("^(DEBIT|FEE)$"), "CORE").otherwise("PRIORITY").alias("txn_type_zone"),
    )
    .orderBy(F.col("status").asc_nulls_last(), "num_rows")
)
