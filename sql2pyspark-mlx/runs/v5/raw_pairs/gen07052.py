from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit") <= 8300)
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("c.segment"),
        F.col("c.age"),
        F.regexp_replace(F.col("c.name"), " ", "-").alias("name_slug"),
    )
)
