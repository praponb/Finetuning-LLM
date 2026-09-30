from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .filter(F.col("device").isin(["tablet", "mobile", "desktop"]))
    .groupBy("customer_id")
    .agg(F.countDistinct("country").alias("unique_countrys"))
)

result = (
    df_customers.alias("c")
    .join(sessions_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.age"), F.col("t.unique_countrys"))
    .orderBy("age")
)
