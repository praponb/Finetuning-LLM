from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("c.is_active"))
    .select(
        F.col("a.balance"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("c.city"),
        F.when(F.col("c.city").isin(["New York", "Singapore"]), "Core").otherwise("Other").alias("city_group"),
    )
)
