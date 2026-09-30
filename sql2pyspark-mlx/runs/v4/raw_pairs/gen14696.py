from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter((F.col("a.balance") >= 62000) & (F.col("a.balance") > 43000))
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("c.city"),
        F.col("c.name"),
        F.col("c.age"),
    )
    .orderBy("status")
)
