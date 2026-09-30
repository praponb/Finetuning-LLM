from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.hour("txn_time").between(10, 15))
    .select("*", F.col("is_flagged").cast("int").alias("is_flagged_flag"))
)

flagged = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy("merchant")
    .agg(
        F.max("txn_time").alias("last_seen"),
        F.max("txn_time").alias("ended_at"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
    )
    .filter(F.col("amount_p90") >= 0)
    .orderBy(F.col("amount_p90").asc_nulls_last())
)
