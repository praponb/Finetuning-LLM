from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.status").isin(["OPEN", "FROZEN"]) & (F.col("c.age") <= 34))
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("c.email"),
        F.col("c.city"),
        F.col("c.credit_limit"),
    )
)
