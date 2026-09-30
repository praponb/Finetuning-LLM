from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("year_start").orderBy(F.col("balance_stddev").desc())

eligible = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date")), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.date_trunc("year", "opened_date").alias("year_start"),
        "account_type",
        "status",
    )
    .agg(
        F.stddev_samp("balance").alias("balance_stddev"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_rows"),
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
        F.stddev_samp("balance").alias("balance_stddev_2"),
    )
    .withColumn("balance_stddev_dense_rank", F.dense_rank().over(w))
)
