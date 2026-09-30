from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy("amount_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy("amount_avg_7d", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("txn_type").orderBy("last_known_amount_rounded", "id")
w_running = Window.partitionBy("status").orderBy("amount_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("txn_type", "account_type")
w_grp = Window.partitionBy("account_id", "status").orderBy("adjusted_balance", "id")
w8 = Window.partitionBy("account_id").orderBy(F.col("balance_avg_6").desc(), "id")
w9 = Window.partitionBy("account_id").orderBy(F.col("amount_avg_7d").desc_nulls_last())
w10 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w11 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.round("amount", 2).alias("amount_rounded"),
        F.date_format("txn_time", "MMM yyyy").alias("txn_time_label"),
    )
)

with_windows = (
    filtered
    .withColumn("amount_avg_7d", F.avg("amount").over(w))
    .withColumn("last_known_amount_rounded", F.last("amount_rounded", ignorenulls=True).over(w2))
)

joined = (
    with_windows
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.account_type"), F.col("p.status"), F.col("p.balance"))
)

filtered_transactions = (
    joined
    .filter((F.col("txn_time") > F.lit("2024-01-13").cast("date")) & (F.col("amount_avg_7d") < 1379))
    .select(
        "*",
        F.least("amount_avg_7d", "amount_rounded").alias("least_amount_avg_7d_amount_rounded"),
        F.round((F.col("balance") * 0.85), 2).alias("adjusted_balance"),
    )
)

scored = (
    filtered_transactions
    .select(
        "*",
        F.avg("balance").over(w3).alias("balance_avg_6"),
        F.lead("amount_avg_7d", 1).over(w4).alias("next_amount_avg_7d"),
        F.sum("amount_rounded").over(w_running).alias("running_amount_rounded"),
        F.sum("amount_avg_7d").over(w6).alias("txn_type_sum_amount_avg_7d"),
        F.ntile(4).over(w_grp).alias("adjusted_balance_bucket"),
    )
)

enriched = (
    scored
    .withColumn("balance_avg_6_bucket", F.ntile(4).over(w8))
    .withColumn("amount_avg_7d_percent_rank", F.percent_rank().over(w9))
)

gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w10).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w10) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("island_id", F.sum("is_new_session").over(w11))

pivoted = (
    sessions_transactions
    .groupBy("account_type")
    .pivot("txn_type", ["FEE", "CREDIT"])
    .agg(F.sum("balance").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)

result = (
    pivoted
    .select(
        "account_type",
        "FEE_amount",
        "CREDIT_amount",
        "CREDIT_cnt",
        F.when(F.col("account_type").rlike("^(SAVINGS|CREDIT)$"), "EXTENDED").otherwise("CORE").alias("account_type_zone"),
    )
)
