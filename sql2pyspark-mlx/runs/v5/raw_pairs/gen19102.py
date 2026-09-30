from pyspark.sql import functions as F

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("balance") >= 88000, "LARGE").otherwise("SMALL").alias("balance_tier"),
    )
    .agg(
        F.count_if(F.col("status") == "FROZEN").alias("frozen_events"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("cnt"),
    )
)
