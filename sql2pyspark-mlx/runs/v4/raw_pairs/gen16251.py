from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.filter(F.col("balance") > 72000).alias("accounts"), F.col("customers.id") == F.col("accounts.customer_id"), "left_anti")
    .filter(F.col("customers.country") == "TH")
    .select(F.col("customers.id"), F.col("customers.city"))
    .orderBy(F.col("id").desc())
    .limit(20)
)
