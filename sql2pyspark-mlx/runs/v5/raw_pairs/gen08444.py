from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.segment") != "SMB") & F.col("s.customer_id").isNotNull())
    .select(F.col("s.id"), F.col("s.country"), F.col("c.age"))
    .orderBy(F.col("age").desc(), "id")
    .limit(5)
)
