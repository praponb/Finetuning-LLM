from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.signup_date").between("2023-09-01", "2025-07-01"))
    .select(F.col("c.age"), F.col("s.browser"), F.col("s.started_at"))
    .orderBy(F.col("started_at").desc())
)
