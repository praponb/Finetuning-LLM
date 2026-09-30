from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("txn_type").orderBy("running_balance", "id").rowsBetween(-6, Window.currentRow)

metrics = (
    df_accounts
    .select(
        "id",
        "account_type",
        F.sum("balance").over(w).alias("running_balance"),
        F.first("status", ignorenulls=True).over(w_part).alias("first_known_status"),
        F.avg("balance").over(w_grp).alias("balance_avg_2"),
        F.sum("balance").over(w4).alias("cumulative_balance"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.running_balance",
        "s.first_known_status",
        "s.cumulative_balance",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.txn_type"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.transaction_txn_time")), "left_anti")
)

metrics_accounts = (
    kept
    .withColumn("last7_max_running_balance", F.max("running_balance").over(w_hist))
    .withColumn("last7_avg_running_balance", F.avg("running_balance").over(w_hist))
    .withColumn("last7_min_running_balance", F.min("running_balance").over(w_hist))
)

result = (
    metrics_accounts
    .select(
        "id",
        "account_type",
        "running_balance",
        "first_known_status",
        "txn_type",
        "last7_avg_running_balance",
    )
    .orderBy(F.col("txn_type").desc())
)
