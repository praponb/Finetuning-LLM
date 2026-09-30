from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.col("segment").isin(["Consumer", "Corporate", "SMB"])).select("id")

result = (
    df_sessions.alias("s")
    .join(excluded_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("s.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .select(F.col("s.id"), F.col("s.started_at"), F.col("s.device"))
    .orderBy(F.col("device").desc(), F.col("id").desc())
    .limit(5)
)
