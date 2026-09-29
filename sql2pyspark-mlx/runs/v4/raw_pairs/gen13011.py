from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("YEAR", "paid_at").alias("year_start"))
    .agg(
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.sort_array(F.collect_set("method")).alias("method_list_2"),
        F.round(F.avg("new_group_flag"), 2).alias("avg_new_group_flag"),
    )
)
