from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(~((F.col("o.status") == "RETURNED") & F.col("o.channel").isin(["store", "mobile"])))
    .select(
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("p.order_id"),
        F.col("p.amount"),
        F.date_trunc("week", F.col("o.order_date")).alias("order_date_period"),
    )
    .orderBy("order_date")
)
