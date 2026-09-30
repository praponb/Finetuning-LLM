from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "right")
    .groupBy(
        F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"),
        F.quarter(F.col("o.order_date")).alias("order_quarter"),
    )
    .agg(F.sum(F.col("p.amount")).alias("total_amount"))
    .filter(F.col("total_amount") >= 2900)
    .orderBy("total_amount")
)
