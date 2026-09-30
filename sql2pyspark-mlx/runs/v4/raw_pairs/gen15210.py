from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("orders.channel").isin(["partner", "mobile"]))
    .groupBy(F.col("orders.status"), F.col("orders.customer_id"))
    .agg(
        F.countDistinct(F.col("payments.amount")).alias("unique_methods"),
        F.min(F.col("payments.amount")).alias("min_amount"),
    )
    .orderBy(F.col("status").desc())
)
