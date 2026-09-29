from pyspark.sql import functions as F

kept = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") < F.col("s.signup_date")), "left_anti")
)

result = (
    kept
    .select("id", "name", "email", "signup_date", "credit_limit", "age")
    .orderBy(F.col("id").desc_nulls_first())
)
