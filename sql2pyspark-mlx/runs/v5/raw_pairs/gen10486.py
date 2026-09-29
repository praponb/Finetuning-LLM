from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.alias("accounts"), F.col("customers.id") == F.col("accounts.customer_id"), "left_semi")
    .filter(F.col("customers.city").isin(["Berlin", "New York"]))
    .select(
        F.col("customers.id"),
        F.col("customers.segment"),
        F.col("customers.signup_date"),
        F.col("customers.age"),
    )
    .orderBy("signup_date")
)
