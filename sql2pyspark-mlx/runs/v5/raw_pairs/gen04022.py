from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-07-08 06:00:00"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.trim("txn_type").alias("clean_txn_type"),
    )
)

prev_events = cleaned.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        "clean_txn_type",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .groupBy("account_id")
    .agg(
        F.percentile_approx("session_no", 0.5).alias("session_no_p50"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.max("txn_time").alias("last_seen"),
    )
    .orderBy(F.col("session_no_p50").asc_nulls_first())
)
