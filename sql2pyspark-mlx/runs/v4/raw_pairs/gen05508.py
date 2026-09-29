from pyspark.sql import functions as F

kept = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.account_type") == "LOAN"), "left_anti")
)

result = kept.select("id", "name", "email", "city", "signup_date", "is_active", "age")
