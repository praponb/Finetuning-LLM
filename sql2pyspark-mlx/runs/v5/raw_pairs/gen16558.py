from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status")
w2 = Window.partitionBy("account_type", "customer_id")
w3 = Window.orderBy(F.col("balance").desc_nulls_first())
w4 = Window.partitionBy("customer_id")
w_part = Window.orderBy(F.col("balance").desc())
w_part_2 = Window.partitionBy("status", "customer_id").orderBy("account_type_stddev_samp_balance")

kept_keys = (
    df_accounts
    .select("account_type", "customer_id")
    .exceptAll(
        df_accounts
        .filter(~F.col("account_type").isin(["CREDIT", "SAVINGS", "CHECKING"]))
        .select("account_type", "customer_id")
    )
)

reconciled = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

with_windows = (
    eligible
    .withColumn("customer_id_avg_balance", F.avg("balance").over(w))
    .withColumn("account_type_stddev_samp_balance", F.stddev_samp("balance").over(w2))
    .withColumn("balance_dense_rank", F.dense_rank().over(w3))
    .withColumn("customer_id_count_balance", F.count("balance").over(w4))
)

scored = (
    with_windows
    .withColumn("customer_id_sum_account_type_stddev_samp_balance", F.sum("account_type_stddev_samp_balance").over(w))
    .withColumn("balance_rank", F.rank().over(w_part))
    .withColumn("account_type_stddev_samp_balance_percent_rank", F.percent_rank().over(w_part_2))
    .withColumn("balance_dense_rank_share", F.round((F.col("balance_dense_rank") / F.sum("balance_dense_rank").over(w4)), 4))
    .withColumn("customer_id_min_customer_id_avg_balance", F.min("customer_id_avg_balance").over(w4))
)

result = (
    scored
    .groupBy(
        F.date_trunc("week", "opened_date").alias("week_start"),
        F.when(F.col("balance_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("balance_rank_tier"),
        F.date_trunc("quarter", "opened_date").alias("quarter_start"),
    )
    .agg(
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("balance_rank_tier").asc_nulls_first())
)
