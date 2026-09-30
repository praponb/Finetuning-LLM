from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.status").isin(["FROZEN", "CLOSED", "OPEN"]))
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("c.segment"),
        F.col("c.email"),
        F.col("c.country"),
        F.when(F.col("c.is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
