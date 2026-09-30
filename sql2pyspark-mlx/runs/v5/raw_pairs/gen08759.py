from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_transactions = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_transactions
    .withColumn("year_start", F.date_trunc("YEAR", "txn_time"))
    .groupingSets(
        [["year_start", "burst_id", "txn_type"], ["year_start", "burst_id"], ["year_start"], []],
        "year_start",
        "burst_id",
        "txn_type",
    )
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("burst_id")).otherwise(0)).alias("refund_burst_id"),
        F.min("txn_time").alias("started_at"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("txn_type") == "DEBIT", 1)), F.lit(0)), 2).alias("amount_per_debit"),
    )
    .orderBy(F.col("txn_type").desc_nulls_first())
)
