from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("txn_type", "account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("txn_time", "id")
w3 = Window.partitionBy("status").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_transactions
    .withColumn("running_min_amount", F.min("amount").over(w_hist))
    .withColumn("running_count_amount", F.count("amount").over(w_hist))
    .withColumn("running_max_amount", F.max("amount").over(w_hist))
)

enriched = (
    metrics
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.balance"), F.col("d.opened_date"))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.account_id",
        "src.txn_time",
        "src.running_min_amount",
        "src.running_count_amount",
        "src.running_max_amount",
        "src.balance",
        "src.opened_date",
        F.col("d.status"),
    )
)

gaps = (
    with_dims
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w2).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w2) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "txn_time",
        "running_min_amount",
        "opened_date",
        "status",
        F.sum("new_group_flag").over(w3).alias("session_no"),
    )
)

result = islands.select("id", "txn_time", "running_min_amount", "status", "session_no")
