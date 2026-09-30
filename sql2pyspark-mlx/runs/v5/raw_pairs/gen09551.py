from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select("opened_date")
    .union(df_customers.select("signup_date"))
    .distinct()
    .orderBy("opened_date")
)
