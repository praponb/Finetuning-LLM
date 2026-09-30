from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").asc_nulls_first(), "id")
w2 = Window.orderBy("opened_date", "id")
w3 = Window.partitionBy("status", "customer_id").orderBy(F.col("latest_rank").desc(), "id")

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.is_flagged")), "left_anti")
)

ranked = (
    kept
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .withColumn("prev_balance", F.lag("balance", 1).over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

eligible = (
    enriched
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date")), "left_anti")
)

stats = (
    eligible
    .rollup("seq_no")
    .agg(
        F.avg("seq_no").alias("avg_seq_no"),
        F.round(F.sum("prev_balance") / F.nullif(F.count(F.when(F.col("status") == "FROZEN", 1)), F.lit(0)), 2).alias("prev_balance_per_frozen"),
        F.grouping("seq_no").alias("grouping_id"),
    )
    .filter(F.col("prev_balance_per_frozen") > 0)
)

result = (
    stats
    .select("seq_no", "avg_seq_no", "grouping_id")
    .orderBy(F.col("grouping_id").desc())
)
