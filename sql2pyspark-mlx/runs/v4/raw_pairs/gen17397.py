from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.filter(F.col("account_type") == "CREDIT").alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_anti")
    .filter(~F.col("c.name").like("%Pim%"))
    .select(F.col("c.id"), F.col("c.signup_date"), F.col("c.age"))
    .orderBy(F.col("id").desc())
)
