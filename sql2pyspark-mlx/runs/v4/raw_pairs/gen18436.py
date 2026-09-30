from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("c.country"),
        F.col("c.city"),
        F.col("a.opened_date"),
        F.col("a.account_type"),
    )
)
