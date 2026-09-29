from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter((F.year(F.col("o.order_date")) == 2024) | F.col("o.discount").between(0.1, 0.17))
    .select(
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("o.status"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.when(F.col("o.amount") >= 1700, "High").when(F.col("o.amount") >= 770, "Medium").otherwise("Low").alias("amount_level"),
    )
    .orderBy("store_name")
)
