from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.country"),
        F.col("s.browser"),
        F.col("s.device"),
        F.col("c.name"),
        F.col("c.email"),
        F.col("c.signup_date"),
        F.date_trunc("year", F.col("c.signup_date")).alias("signup_date_period"),
    )
)
