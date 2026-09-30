from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("a.balance") <= 61000) & F.col("a.account_type").isin(["CREDIT", "SAVINGS", "CHECKING"]))
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
        F.col("c.city"),
    )
)
