from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("c.email").like("%@startup.io") & (F.col("c.credit_limit") > 13600))
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("c.credit_limit"),
    )
    .orderBy("credit_limit")
)
