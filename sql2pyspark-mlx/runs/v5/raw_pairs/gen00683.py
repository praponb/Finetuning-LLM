from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.name").like("%Dav%"))
    .select(
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("c.is_active"),
        F.col("c.credit_limit"),
        F.col("c.age"),
    )
    .orderBy("customer_id", F.col("credit_limit").desc())
)
