from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(~F.col("s.city").isin(["Tokyo", "New York", "Singapore"]))
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("s.region"),
    )
)
