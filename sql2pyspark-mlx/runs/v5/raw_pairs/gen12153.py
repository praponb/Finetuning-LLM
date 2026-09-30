from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .filter(F.col("c.signup_date") > "2023-04-01")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.plan"),
        F.col("s.start_date"),
        F.col("c.name"),
        F.col("c.signup_date"),
        F.col("c.age"),
        F.upper(F.col("c.name")).alias("name_upper"),
    )
    .orderBy(F.col("plan").desc(), "signup_date")
)
