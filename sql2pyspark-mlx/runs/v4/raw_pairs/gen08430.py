from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

scored = (
    df_payments
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
        F.count("amount").over(w3).alias("method_count_amount"),
    )
)

result = (
    scored
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "last_known_amount",
        "method_count_amount",
        F.col("avg_shipment_cost"),
    )
    .orderBy(F.col("method_count_amount").asc_nulls_first())
)
