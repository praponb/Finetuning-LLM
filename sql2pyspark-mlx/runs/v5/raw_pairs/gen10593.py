from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .groupBy(
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
        F.col("c.country"),
    )
    .agg(F.count("*").alias("cnt"))
    .orderBy(F.col("country").desc())
)
