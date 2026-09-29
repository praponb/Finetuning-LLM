from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("year_start").orderBy(F.col("amazon_events").desc())

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy(
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
        F.when(F.col("burst_id") >= 7, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
        F.when(F.col("new_group_flag") >= 1, "LARGE").otherwise("SMALL").alias("new_group_flag_tier"),
    )
    .agg(
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
        F.count_if(F.col("merchant") == "Amazon").alias("amazon_events"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_rows"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("debit_rows") > 1)
    .withColumn("amazon_events_percent_rank", F.percent_rank().over(w3))
)
