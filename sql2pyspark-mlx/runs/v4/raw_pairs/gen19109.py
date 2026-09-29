from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.filter(F.col("balance") > 62000).alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_semi")
    .filter(~F.col("c.country").isin(["TH", "JP", "US"]))
    .select(F.col("c.id"), F.col("c.credit_limit"), F.col("c.age"))
)
