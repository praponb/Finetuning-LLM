from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .filter(F.col("country") == "SG")
    .groupBy("customer_id")
    .agg(
        F.max("started_at").alias("latest_started_at"),
        F.countDistinct("browser").alias("unique_browsers"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.unique_browsers") > 6)
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("c.segment"),
        F.col("cs.latest_started_at"),
        F.col("cs.unique_browsers"),
    )
    .orderBy(F.col("unique_browsers").desc())
)
