from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.name").like("Oli%") & (F.col("c.age") <= 39))
    .select(
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("c.country").alias("customer_country"),
        F.when(F.col("s.device").isin(["tablet", "mobile"]), "Tier 1").otherwise("Other").alias("device_group"),
    )
)
