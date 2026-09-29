from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    islands
    .groupBy(
        F.when(F.col("burst_id") >= 7, "HIGH").otherwise("LOW").alias("burst_id_tier"),
        "account_id",
        "burst_id",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("burst_id").desc())
)
