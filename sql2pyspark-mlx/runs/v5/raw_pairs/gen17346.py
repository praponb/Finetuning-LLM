from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter((F.col("c.age") <= 35) & (F.col("a.status") == "OPEN"))
    .select(F.col("c.signup_date"), F.col("c.segment"), F.col("a.account_type"))
    .orderBy("account_type", "segment")
)
