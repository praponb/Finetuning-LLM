from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-11-18 12:00:00").cast("timestamp"))
    .withColumn("method_prefix", F.substring("method", 1, 2))
)

gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("island_id") >= 7, "LARGE").otherwise("SMALL").alias("island_id_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max("paid_at").alias("ended_at"),
        F.min("is_new_burst").alias("min_is_new_burst"),
        F.sort_array(F.collect_set("method")).alias("method_list_2"),
    )
    .filter(F.col("num_status") < 1)
    .orderBy(F.col("min_is_new_burst").asc_nulls_last())
)
