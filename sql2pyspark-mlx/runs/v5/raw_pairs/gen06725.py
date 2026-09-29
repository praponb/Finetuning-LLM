from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("c.segment"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.trim(F.col("o.channel")).alias("clean_channel"),
    )
)
