from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .groupBy(F.col("c.country"))
    .agg(
        F.sum(F.col("a.balance")).alias("total_balance"),
        F.min(F.col("a.opened_date")).alias("first_opened_date"),
    )
    .orderBy(F.col("total_balance").desc(), "country")
    .limit(50)
)
