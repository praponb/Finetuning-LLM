from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("c.country"),
        F.col("c.signup_date"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
        F.coalesce(F.col("c.credit_limit"), F.lit(0)).alias("credit_limit_filled"),
    )
    .orderBy("opened_date")
)
