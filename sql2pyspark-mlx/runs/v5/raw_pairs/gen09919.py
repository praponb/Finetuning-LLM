from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.email").like("%@company.co"))
    .select(
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("c.signup_date"),
        F.col("c.country"),
        F.col("c.city"),
    )
)
