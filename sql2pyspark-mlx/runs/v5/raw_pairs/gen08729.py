from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

metrics = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.min("amount").over(w).alias("last4_min_amount"),
        F.sum("amount").over(w).alias("last4_sum_amount"),
        F.max("amount").over(w).alias("last4_max_amount"),
        F.avg("amount").over(w).alias("last4_avg_amount"),
    )
)

result = (
    metrics
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "last4_sum_amount",
        F.when(F.col("status").rlike("^(SUCCESS|REFUNDED)$"), "EXTENDED").when(F.col("status").rlike("^(FAILED)$"), "GROUP_B").otherwise("PRIORITY").alias("status_zone"),
        F.col("max_order_item_unit_price"),
    )
)
