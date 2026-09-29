from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age") <= 39)
    .select(
        F.col("c.name"),
        F.col("c.age"),
        F.col("o.order_date"),
        F.col("o.status"),
        F.concat_ws("-", F.col("c.segment"), F.col("c.city")).alias("segment_city_key"),
    )
    .orderBy(F.col("name").desc())
)
