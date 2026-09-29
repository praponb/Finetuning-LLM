from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .groupBy(
        F.when(F.col("amount") >= 4000, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.count_if(F.col("merchant") == "Shell").alias("shell_events"),
        F.max("txn_time").alias("last_txn_time"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "Grab", 1)), F.lit(0)), 2).alias("amount_per_grab"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("amount_per_grab").asc_nulls_first())
)
