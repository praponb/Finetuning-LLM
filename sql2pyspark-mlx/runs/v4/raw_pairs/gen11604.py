from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("opened_date", "id")
w2 = Window.partitionBy("merchant")
w3 = Window.partitionBy("account_id")
w_ordered = Window.orderBy("amount_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w6 = Window.orderBy(F.col("amount").desc_nulls_last())
w_recent = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id")
w8 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w9 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("account_type").orderBy("txn_time", "id")
w11 = Window.partitionBy("account_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.opened_date"))
)

scored = (
    enriched
    .select(
        "*",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        F.count("amount").over(w2).alias("merchant_count_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
    )
)

metrics = (
    scored
    .withColumn("first_known_amount_share", F.first("amount_share", ignorenulls=True).over(w_ordered))
    .withColumn("amount_bucket", F.ntile(10).over(w5))
    .withColumn("amount_rank", F.rank().over(w6))
    .withColumn("amount_bucket_2", F.ntile(4).over(w_recent))
)

joined = (
    metrics
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.account_type"))
)

flagged = (
    joined
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w8).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w8) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_burst").over(w9))

gaps = (
    sessionized
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w10).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w10) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("session_start_flag").over(w11))

enriched_transactions = (
    islands
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select(
        "s.*",
        F.col("a.opened_date").alias("account_opened_date"),
        F.col("a.balance"),
    )
)

result = (
    enriched_transactions
    .groupBy("txn_type")
    .pivot("account_type", ["CREDIT", "SAVINGS"])
    .agg(F.sum("session_id").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "txn_type",
        F.col("CREDIT_amount").alias("credit_amount"),
        F.col("CREDIT_n").alias("credit_n"),
        F.col("SAVINGS_amount").alias("savings_amount"),
        F.col("SAVINGS_n").alias("savings_n"),
    )
)
