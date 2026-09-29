from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("order_id").isNotNull()).alias("p"), F.col("o.id") == F.col("p.order_id"), "left_semi")
    .filter(F.col("o.discount") >= 0.39)
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("o.status"),
    )
    .orderBy("order_date")
)
