from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "txn_time",
        "merchant",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .groupBy(F.date_trunc("YEAR", "txn_time").alias("year_start"))
    .agg(
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"),
        F.max("txn_time").alias("ended_at"),
        F.max("txn_time").alias("ended_at_2"),
        F.avg(F.when(F.col("merchant") == "Starbucks", F.col("island_id"))).alias("starbucks_avg_island_id"),
        F.round(F.sum("island_id") / F.nullif(F.count(F.when(F.col("merchant") == "Amazon", 1)), F.lit(0)), 2).alias("island_id_per_amazon"),
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("island_id"))).alias("amazon_total_island_id"),
    )
    .filter(F.col("island_id_per_amazon") >= 0)
)
