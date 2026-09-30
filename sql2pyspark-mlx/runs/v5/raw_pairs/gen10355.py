from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .groupBy(
        F.month(F.col("o.order_date")).alias("order_month"),
        F.when(F.col("o.amount") >= 1610, "high").when(F.col("o.amount") >= 390, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("p.method") == "cash", F.col("p.amount")).otherwise(0)).alias("cash_amount"),
    )
    .filter(F.col("cash_amount") >= 74)
)
