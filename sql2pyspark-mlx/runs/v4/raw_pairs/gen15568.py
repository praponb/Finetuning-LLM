from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "status").orderBy(F.col("amount").asc_nulls_first())
w2 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.when(F.col("amount") >= 4500, "High").when(F.col("amount") >= 1500, "Medium").otherwise("Low").alias("amount_tier"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "left")
    .select("src.*", F.col("a.account_type"), F.col("a.status"), F.col("a.opened_date"))
)

metrics = (
    joined
    .select(
        "*",
        F.cume_dist().over(w).alias("amount_cume_dist"),
        F.avg("amount").over(w2).alias("amount_avg_30d"),
    )
)

by_type = (
    metrics
    .groupBy("account_id")
    .pivot("txn_type", ["REFUND", "CREDIT", "FEE"])
    .agg(F.sum("amount_avg_30d"))
)

result = (
    by_type
    .join(amount_by_account_id, on="account_id", how="left")
    .select("account_id", "REFUND", "CREDIT", "FEE", F.col("sum_transaction_amount"))
    .orderBy(F.col("sum_transaction_amount").asc_nulls_last())
)
