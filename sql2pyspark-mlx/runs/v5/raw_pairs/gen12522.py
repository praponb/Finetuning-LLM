from pyspark.sql import functions as F

rollup_totals = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "left")
    .rollup(
        F.col("d.account_type").alias("account_type"),
        F.col("e.merchant").alias("merchant"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("e.amount")), 2).alias("total_amount"),
    )
)

result = (
    rollup_totals
    .select("account_type", "rows_total", "total_amount")
    .orderBy(F.col("rows_total").asc_nulls_first(), F.col("account_type").desc())
)
