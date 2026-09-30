from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("s.browser").isin(["Edge", "Safari"]))
    .select(
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("s.device"),
    )
    .orderBy(F.col("device").desc())
)
