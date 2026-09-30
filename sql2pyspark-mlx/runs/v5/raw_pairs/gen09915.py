from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy(F.col("amount").asc_nulls_first())
w4 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("txn_type").orderBy("amount_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("amount_cume_dist").desc(), "id")
w_part = Window.partitionBy("merchant").orderBy("running_sum_amount_avg_2", "id").rowsBetween(-6, -1)
w9 = Window.partitionBy("merchant").orderBy(F.col("running_max_amount_avg_2").desc(), "id")
w10 = Window.partitionBy("txn_type").orderBy("running_amount_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_transactions
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_2"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
        F.cume_dist().over(w3).alias("amount_cume_dist"),
        F.sum("amount").over(w4).alias("running_amount"),
        F.sum("amount").over(w5).alias("running_amount_2"),
    )
)

with_windows_transactions = (
    with_windows
    .withColumn("running_max_amount_avg_2", F.max("amount_avg_2").over(w6))
    .withColumn("running_avg_amount_avg_2", F.avg("amount_avg_2").over(w6))
    .withColumn("running_sum_amount_avg_2", F.sum("amount_avg_2").over(w6))
    .withColumn("seq_no", F.row_number().over(w7))
)

enriched = (
    with_windows_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "last_known_amount",
        "running_max_amount_avg_2",
        F.avg("last_known_amount").over(w_part).alias("last_known_amount_avg_6_prior"),
        F.row_number().over(w9).alias("row_seq"),
        F.last("running_amount", ignorenulls=True).over(w10).alias("last_known_running_amount"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.amount",
        "a.last_known_amount",
        "a.last_known_amount_avg_6_prior",
        F.col("b.status"),
        F.col("b.opened_date"),
    )
)

result = (
    with_dims
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .groupBy(
        F.col("p.account_type").alias("account_type"),
        F.col("e.status").alias("transaction_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("e.last_known_amount_avg_6_prior")).alias("total_last_known_amount_avg_6_prior"),
    )
    .orderBy(F.col("total_last_known_amount_avg_6_prior").asc_nulls_first())
)
