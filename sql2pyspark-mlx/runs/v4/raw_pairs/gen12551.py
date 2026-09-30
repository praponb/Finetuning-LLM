from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("a.account_type").isin(["SAVINGS", "CHECKING", "LOAN"]))
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("c.name"),
        F.col("c.email"),
        F.col("c.city"),
    )
    .orderBy("email", "name")
)
