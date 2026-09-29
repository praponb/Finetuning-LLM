from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter((F.col("o.discount") > 0.11) & (F.length(F.col("c.name")) > 8))
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("c.credit_limit"),
        F.col("c.segment"),
        F.concat_ws("/", F.col("c.name"), F.col("c.city")).alias("name_city_key"),
    )
    .orderBy(F.col("sqft").desc(), "city")
)
