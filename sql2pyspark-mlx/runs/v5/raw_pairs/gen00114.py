from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-08-28 21:00:00"))) & F.col("merchant").isNotNull())
    .select("*", F.col("txn_time").cast("date").alias("txn_time_day"))
)

with_gaps = (
    base
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

result = (
    sessionized
    .groupBy(F.date_trunc("month", "txn_time").alias("activity_month"))
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.min("session_no").alias("min_session_no"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "Amazon", 1)), F.lit(0)), 2).alias("amount_per_amazon"),
    )
    .filter(F.col("span_minutes") > 0)
    .orderBy(F.col("span_minutes").desc_nulls_last())
)
