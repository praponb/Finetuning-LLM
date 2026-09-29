from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.when(F.col("amount") >= 4100, "High").when(F.col("amount") >= 600, "Medium").otherwise("Low").alias("amount_tier"),
    )
)

with_gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .rollup(F.date_trunc("YEAR", "txn_time").alias("year_start"))
    .agg(
        F.percentile_approx("session_start_flag", [0.25, 0.5, 0.75]).alias("session_start_flag_quartiles"),
        F.count("*").alias("num_rows"),
        F.sum("session_no").alias("total_session_no"),
    )
    .orderBy(F.col("year_start").asc_nulls_last())
)
