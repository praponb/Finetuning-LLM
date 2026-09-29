from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter((F.col("c.signup_date") > "2024-03-03") | (F.col("a.account_type") == "CHECKING"))
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("c.name"), F.col("c.city"))
)
