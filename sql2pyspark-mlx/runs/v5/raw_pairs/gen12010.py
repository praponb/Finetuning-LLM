from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_status").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_status").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select("*", F.col("is_flagged").cast("int").alias("is_flagged_flag"))
)

enriched = (
    valid
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "inner")
    .filter(F.col("d.opened_date").between("2023-12-01", "2025-07-01"))
    .select("s.*", F.col("d.account_type"))
)

joined = (
    enriched
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.status"), F.col("p.balance"))
)

joined_transactions = (
    joined
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.status").alias("account_status"),
        F.col("p.account_type").alias("account_account_type"),
    )
)

gaps = (
    joined_transactions
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_flagged_flag",
        "account_type",
        "status",
        "balance",
        "account_status",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .groupBy("status")
    .pivot("merchant", ["Shell", "Starbucks", "Amazon"])
    .agg(F.sum("burst_id").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "status",
        F.col("Shell_value_sum").alias("shell_value_sum"),
        F.col("Shell_cnt").alias("shell_cnt"),
        F.col("Starbucks_value_sum").alias("starbucks_value_sum"),
        F.col("Starbucks_cnt").alias("starbucks_cnt"),
        F.col("Amazon_value_sum").alias("amazon_value_sum"),
        F.col("Amazon_cnt").alias("amazon_cnt"),
    )
)
