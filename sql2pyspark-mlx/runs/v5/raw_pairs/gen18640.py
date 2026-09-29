from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.city").isin(["Bangkok", "Chiang Mai"]))
    .select(F.col("a.id"), F.col("a.balance"), F.col("c.city"))
)
