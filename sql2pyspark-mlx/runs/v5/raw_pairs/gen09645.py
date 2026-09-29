from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w3 = Window.partitionBy("method", "order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id", "status").orderBy(F.col("amount").desc())
w5 = Window.partitionBy("status", "order_id").orderBy(F.col("amount").desc(), "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.avg("quantity").alias("avg_order_item_quantity"))

scored = (
    df_payments
    .select(
        "id",
        "order_id",
        "status",
        F.avg("amount").over(w).alias("amount_avg_2"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
        F.first("amount", ignorenulls=True).over(w3).alias("first_known_amount"),
        F.percent_rank().over(w4).alias("amount_percent_rank"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

result = (
    scored
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "status",
        "amount_avg_2",
        "next_amount",
        "first_known_amount",
        "amount_percent_rank",
        "event_seq",
        F.col("avg_order_item_quantity"),
    )
)
