from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .groupBy(
        F.col("a.customer_id"),
        F.quarter(F.col("a.opened_date")).alias("opened_quarter"),
    )
    .agg(
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("t.txn_type")))).alias("txn_type_values"),
    )
    .orderBy(F.col("opened_quarter").desc())
)
