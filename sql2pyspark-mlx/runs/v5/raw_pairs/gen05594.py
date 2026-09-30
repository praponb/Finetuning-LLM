from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "outer")
    .groupBy(
        F.when(F.col("customers.age") >= 68, "high").when(F.col("customers.age") >= 27, "mid").otherwise("low").alias("age_bucket"),
        F.quarter(F.col("customers.signup_date")).alias("signup_quarter"),
    )
    .agg(F.count(F.when(F.col("accounts.status") == "OPEN", 1)).alias("open_count"))
)
