from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("txn_time") >= F.to_date(F.lit("2023-05-06")))
    .select("*", F.col("amount").cast("int").alias("amount_int"))
)

with_prev = base.withColumn("prev_txn_time", F.lag("txn_time").over(w))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "amount_int",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessionized
    .groupBy(F.date_trunc("YEAR", "txn_time").alias("year_start"), "merchant")
    .agg(
        F.percentile_approx("amount_int", 0.95).alias("amount_int_p95"),
        F.percentile_approx("session_no", [0.25, 0.5, 0.75]).alias("session_no_quartiles"),
        F.min("txn_time").alias("first_seen"),
        F.max(F.when(F.col("merchant") == "Lazada", F.col("amount_int"))).alias("lazada_max_amount_int"),
        F.max(F.when(F.col("merchant") == "7-Eleven", F.col("amount_int"))).alias("7_eleven_max_amount_int"),
    )
)
