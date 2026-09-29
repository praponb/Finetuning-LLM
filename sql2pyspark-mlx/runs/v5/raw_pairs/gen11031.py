from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("a.balance"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("c.city"),
        F.col("c.name"),
        F.when(F.col("c.segment").isin(["Corporate", "Consumer"]), "Priority").otherwise("Other").alias("segment_group"),
    )
    .orderBy(F.col("segment_group").desc())
)
