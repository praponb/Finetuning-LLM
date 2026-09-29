from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        F.round("amount", 1).alias("amount_rounded"),
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .groupBy(
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
    )
    .agg(
        F.percentile_approx("amount_filled", [0.25, 0.5, 0.75]).alias("amount_filled_quartiles"),
        F.percentile_approx("amount_filled", 0.5).alias("amount_filled_p50"),
        F.count("*").alias("row_count"),
        F.min("amount").alias("min_amount"),
        F.min("txn_time").alias("first_seen"),
        F.max("txn_time").alias("ended_at"),
    )
    .filter(F.col("min_amount") < 0)
)
