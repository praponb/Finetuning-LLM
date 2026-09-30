from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.store_name").isNotNull())
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg((F.max(F.col("o.amount")) - F.min(F.col("o.amount"))).alias("amount_range"))
    .filter(F.col("amount_range") > 36)
    .orderBy(F.col("opened_year").desc())
)
