from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("c.age"),
        F.col("c.city"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("credit_limit").desc())
)
