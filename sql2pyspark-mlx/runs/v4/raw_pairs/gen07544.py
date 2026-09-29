from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("amount").alias("avg_order_amount"))

flagged = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "browser", "country", F.col("avg_order_amount"))
    .orderBy(F.col("country").desc())
)
