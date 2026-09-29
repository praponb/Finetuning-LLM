from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter((F.col("s.sqft") < 10648) & (F.col("o.amount") > 1060))
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("s.store_name"),
    )
)
