from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(~F.col("p.method").isin(["bank_transfer", "paypal", "cash"]) | F.col("o.order_date").between("2023-11-09", "2024-08-01"))
    .select(
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
        F.col("p.paid_at"),
        F.col("p.amount"),
    )
)
