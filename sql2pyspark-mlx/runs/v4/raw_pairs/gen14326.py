from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("c.credit_limit"),
        F.col("c.country"),
    )
    .orderBy("country", "id")
    .limit(5)
)
