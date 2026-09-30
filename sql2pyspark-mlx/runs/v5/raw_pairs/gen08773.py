from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("amount", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

scored = (
    df_payments
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_3_prior"),
        F.avg("amount").over(w2).alias("amount_avg_90d"),
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
        "amount_avg_3_prior",
        "amount_avg_90d",
        F.col("sum_shipment_cost"),
    )
)
