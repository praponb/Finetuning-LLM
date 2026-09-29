from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.filter(F.col("opened_date") >= "2023-12-01").alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_semi")
    .filter(F.col("c.signup_date") < "2023-06-01")
    .select(F.col("c.id"), F.col("c.is_active"), F.col("c.credit_limit"))
    .orderBy(F.col("credit_limit").desc(), F.col("id").desc())
)
