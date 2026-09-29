from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("amount_whole").desc())
w3 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("amount").desc(), "id")
w4 = Window.orderBy("amount_whole_rank", "id")
w5 = Window.partitionBy("account_id", "txn_type")
w_running = Window.partitionBy("txn_type").orderBy("prev_amount_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running_2 = Window.partitionBy("account_id").orderBy("running_amount_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("merchant") != "Grab"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.substring("txn_type", 1, 3).alias("txn_type_prefix"),
    )
)

valid = (
    base
    .filter(~F.col("merchant").isin(["Lazada", "Starbucks"]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_type_prefix",
        F.floor("amount").alias("amount_whole"),
    )
)

enriched = (
    valid
    .withColumn("running_amount_whole", F.sum("amount_whole").over(w_grp))
    .withColumn("amount_whole_rank", F.rank().over(w2))
)

with_windows = (
    enriched
    .withColumn("amount_bucket", F.ntile(3).over(w3))
    .withColumn("prev_amount_whole", F.lag("amount_whole", 1).over(w4))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w5)), 4))
)

enriched_transactions = (
    with_windows
    .select(
        "*",
        F.first("amount_bucket", ignorenulls=True).over(w_running).alias("first_known_amount_bucket"),
        F.first("running_amount_whole", ignorenulls=True).over(w_running_2).alias("first_known_running_amount_whole"),
    )
)

result = (
    enriched_transactions
    .groupBy("txn_type")
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("amount_whole_rank", 0.95).alias("amount_whole_rank_p95"),
    )
    .filter(F.col("amount_whole_rank_p95") < 0)
    .orderBy(F.col("amount_whole_rank_p95").desc_nulls_first())
)
