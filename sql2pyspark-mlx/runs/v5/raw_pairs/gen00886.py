from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("c.signup_date").between("2023-06-17", "2025-02-08") | F.upper(F.col("c.name")).like("%RAC%"))
    .select(F.col("o.channel"), F.col("c.name"), F.col("s.sqft"))
    .orderBy(F.col("name").desc())
)
