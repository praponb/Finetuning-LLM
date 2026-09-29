from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("account_id", "merchant").orderBy("burst_id", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy("burst_id", "id")

filtered = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .withColumn("txn_time_year", F.year("txn_time"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("DIRECT")))
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

scored = (
    sessions_transactions
    .select(
        "*",
        F.max("burst_id").over(w_recent).alias("last7_max_burst_id"),
        F.min("burst_id").over(w_recent).alias("last7_min_burst_id"),
        F.sum("burst_id").over(w_recent).alias("last7_sum_burst_id"),
        F.lead("new_group_flag", 2).over(w4).alias("next_new_group_flag"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .filter(F.col("p.customer_id").isNull())
    .select("e.*", F.col("p.balance"), F.col("p.account_type"))
)

result = (
    joined
    .select(
        "id",
        "amount",
        "is_flagged",
        "txn_time_year",
        "new_group_flag",
        "burst_id",
        "last7_min_burst_id",
        "next_new_group_flag",
        "balance",
    )
    .orderBy(F.col("txn_time_year").desc(), "next_new_group_flag")
)
