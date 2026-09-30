from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.channel") != "store")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("s.sqft"),
        F.col("c.credit_limit"),
        F.col("c.name"),
    )
    .orderBy(F.col("id").desc(), "name")
)
