from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id", "txn_type").orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy(F.col("txn_type_length").desc(), "id")
w5 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w6 = Window.partitionBy("account_id").orderBy(F.col("is_new_session").desc_nulls_first())
w7 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w8 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("account_id").isin([11, 27, 32, 37]) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.length("txn_type").alias("txn_type_length"),
        F.coalesce("merchant", F.lit("N/A")).alias("resolved_merchant"),
    )
)

joined = (
    filtered
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "inner")
    .filter(F.col("d.customer_id").isin([5, 13]))
    .select("e.*", F.col("d.opened_date"), F.col("d.account_type"))
)

gaps = (
    joined
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

scored = (
    islands
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "txn_type_length",
        "resolved_merchant",
        "account_type",
        "is_new_session",
        F.first("account_type", ignorenulls=True).over(w3).alias("first_known_account_type"),
        F.ntile(3).over(w_part).alias("txn_type_length_bucket"),
        F.lead("txn_type_length", 1).over(w5).alias("next_txn_type_length"),
        F.rank().over(w6).alias("is_new_session_rank"),
    )
)

flagged = (
    scored
    .withColumn("is_new_session_2", F.when(F.col("txn_time") - F.lag("txn_time").over(w7) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("session_id", F.sum("is_new_session_2").over(w8))

result = (
    sessions_transactions
    .groupBy(
        F.when(F.col("txn_type_length_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("txn_type_length_bucket_tier"),
    )
    .agg(
        F.avg(F.when(F.col("account_type") == "CREDIT", F.col("txn_type_length_bucket"))).alias("credit_avg_txn_type_length_bucket"),
        F.percentile_approx("txn_type_length_bucket", 0.9).alias("txn_type_length_bucket_p90"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("account_type") == "CHECKING", 1)), F.lit(0)), 2).alias("session_id_per_checking"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 1)
    .orderBy(F.col("cnt").desc_nulls_last())
)
