from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.upper(F.trim(F.col("s.store_name"))) == "DOWNTOWN EXPRESS")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.status"),
        F.col("s.opened_date"),
    )
)
