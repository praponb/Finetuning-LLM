from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "session_id",
        F.col("sum_order_item_quantity"),
    )
    .orderBy(F.col("order_id").desc())
)
