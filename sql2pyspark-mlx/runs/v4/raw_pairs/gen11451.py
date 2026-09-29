from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy(F.col("total_amount").desc())

valid = (
    df_transactions
    .filter(F.col("is_flagged"))
    .withColumn("merchant_prefix", F.substring("merchant", 1, 2))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("UNKNOWN")))
)

flagged = (
    valid
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

matched_keys = (
    islands
    .select("id", "account_id")
    .subtract(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id", "account_id")
    )
)

verified = (
    islands
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

grouped = (
    verified
    .groupBy("account_id", F.date_trunc("WEEK", "txn_time").alias("week_start"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.percentile_approx("session_no", 0.5).alias("session_no_p50"),
    )
    .withColumn("total_amount_rank", F.rank().over(w3))
)

result = (
    grouped
    .select(
        "account_id",
        "week_start",
        "total_amount",
        "session_no_p50",
        "total_amount_rank",
    )
    .orderBy(F.col("week_start").desc_nulls_first(), F.col("session_no_p50").desc())
)
