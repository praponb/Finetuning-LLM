from pyspark.sql import functions as F

enriched = (
    df_payments
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.status").alias("order_status"),
        F.col("b.amount").alias("order_amount"),
    )
)

result = (
    enriched
    .select("id", "order_id", "method", "amount", "order_status", "order_amount")
    .orderBy(F.col("id").desc_nulls_last(), F.col("method").desc())
)
