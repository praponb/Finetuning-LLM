from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("a.account_type").isin(["CHECKING", "LOAN", "CREDIT"]))
    .select(
        F.col("c.country"),
        F.col("c.is_active"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
    )
)
