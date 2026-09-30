from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

result = (
    islands
    .groupBy(
        F.when(F.col("session_id") >= 8, "HIGH").otherwise("LOW").alias("session_id_tier"),
        "merchant",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("txn_type") == "CREDIT", 1)), F.lit(0)), 2).alias("amount_per_credit"),
        F.max("txn_time").alias("ended_at"),
        F.max("txn_time").alias("last_txn_time"),
        F.countDistinct("txn_type").alias("distinct_txn_types"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("distinct_txn_types").desc())
)
