from pyspark.sql import functions as F

eligible = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id") == 37), "left_anti")
)

result = (
    eligible
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.stddev_samp("balance").alias("balance_stddev"),
    )
    .orderBy(F.col("year_start").asc_nulls_last())
)
