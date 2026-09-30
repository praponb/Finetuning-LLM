from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.col("signup_date") >= "2023-12-01").select("id")

result = (
    df_accounts.alias("a")
    .join(excluded_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("a.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("a.account_type"))
    .orderBy("opened_date")
)
