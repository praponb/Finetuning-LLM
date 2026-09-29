from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("amount").alias("sum_order_amount"))

gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "session_start_flag",
        F.col("sum_order_amount"),
    )
)
