from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("c.name"),
        F.col("c.email"),
        F.trim(F.col("c.country")).alias("clean_country"),
    )
)
