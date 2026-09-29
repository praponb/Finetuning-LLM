from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.status") != "PENDING")
    .groupBy(F.year(F.col("o.order_date")).alias("order_year"))
    .agg(
        F.max(F.col("p.amount")).alias("max_amount"),
        F.min(F.col("p.amount")).alias("min_amount"),
    )
)
