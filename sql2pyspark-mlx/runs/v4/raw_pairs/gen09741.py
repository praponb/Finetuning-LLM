from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("c.credit_limit"))
    .orderBy(F.col("opened_date").desc(), "id")
)
