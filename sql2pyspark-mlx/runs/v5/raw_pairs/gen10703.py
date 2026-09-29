from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy("customer_id")
    .agg(
        F.round(F.avg("new_group_flag"), 2).alias("avg_new_group_flag"),
        F.min("started_at").alias("first_started_at"),
        F.sum("new_group_flag").alias("total_new_group_flag"),
        F.min("started_at").alias("first_seen"),
    )
    .filter(F.col("avg_new_group_flag") < 1000000)
    .orderBy(F.col("customer_id").desc_nulls_last())
)
