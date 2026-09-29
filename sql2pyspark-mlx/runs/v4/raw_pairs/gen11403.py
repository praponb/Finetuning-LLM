from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
        F.col("c.country"),
        F.col("c.age"),
        F.upper(F.col("a.account_type")).alias("account_type_upper"),
    )
)
