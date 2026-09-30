from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .withColumn("year_start", F.date_trunc("YEAR", "txn_time"))
    .rollup("year_start")
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_rows"),
        F.percentile_approx("is_new_burst", 0.95).alias("is_new_burst_p95"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.min("txn_time").alias("started_at"),
        F.max("is_new_burst").alias("max_is_new_burst"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("is_new_burst_p95") > 0)
    .orderBy(F.col("amount_stddev").desc())
)
