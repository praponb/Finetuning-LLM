from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("c.age") != 57)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("c.is_active"),
        F.col("c.age"),
    )
    .orderBy(F.col("age").desc())
)
