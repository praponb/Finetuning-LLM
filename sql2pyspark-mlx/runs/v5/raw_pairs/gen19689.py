from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("WEEK", "txn_time").alias("week_start"))
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("island_id", 0.9).alias("island_id_p90"),
        F.min("is_new_session").alias("min_is_new_session"),
        F.percentile_approx("is_new_session", [0.25, 0.5, 0.75]).alias("is_new_session_quartiles"),
    )
    .filter(F.col("island_id_p90") > 0)
    .orderBy(F.col("week_start").desc_nulls_last())
)
