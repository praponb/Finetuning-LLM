from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.col("credit_limit").between(10200, 15600)).select("id")

result = (
    df_sessions.alias("s")
    .join(excluded_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("s.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .filter(F.hour(F.col("s.started_at")).between(6, 12))
    .select(F.col("s.id"), F.col("s.customer_id"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
