from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("year", "paid_at").alias("year_start"))
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.countDistinct("method").alias("distinct_methods"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_rows"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
    )
    .filter(F.col("avg_island_id") < 0)
    .orderBy(F.col("cash_rows").desc_nulls_first())
)
