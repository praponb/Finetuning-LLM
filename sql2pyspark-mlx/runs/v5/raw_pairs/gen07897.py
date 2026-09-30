from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.email"), F.col("p.name"))
)
