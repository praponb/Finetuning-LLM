from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.col("country") == "SG")
    .groupBy("customer_id")
    .agg(
        F.countDistinct("country").alias("unique_countrys"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(F.col("c.segment"), F.col("ct.unique_countrys"), F.col("ct.cnt"))
)
