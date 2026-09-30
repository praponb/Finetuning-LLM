from pyspark.sql import functions as F

amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

enriched = (
    df_payments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select("s.*", F.col("o.amount").alias("order_amount"), F.col("o.discount"))
)

result = (
    enriched
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "order_amount",
        F.when(F.col("method").rlike("^(promptpay|card)$"), "EXTENDED").otherwise("GROUP_A").alias("method_zone"),
        F.col("sum_payment_amount"),
    )
    .orderBy(F.col("paid_at").asc_nulls_first(), F.col("method").desc())
)
