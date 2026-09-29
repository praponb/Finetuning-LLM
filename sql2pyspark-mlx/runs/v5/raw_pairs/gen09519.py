from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.col("customer_id") == 8)
    .groupBy("customer_id")
    .agg(
        F.countDistinct("browser").alias("unique_browsers"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("C"), F.col("c.id") == F.col("C.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("C.unique_browsers"),
        F.col("C.desktop_count"),
    )
)
