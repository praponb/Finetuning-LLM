from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .filter(F.col("p.amount").between(870, 950) | (F.col("p.paid_at") >= "2023-04-01"))
    .select(
        F.col("p.order_id"),
        F.col("p.status"),
        F.col("p.method"),
        F.col("o.order_date"),
    )
    .orderBy(F.col("method").desc())
)
