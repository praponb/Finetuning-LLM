from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .filter(F.col("C.row_count") >= 8)
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("c.country"),
        F.col("C.desktop_count"),
        F.col("C.row_count"),
    )
    .orderBy(F.col("age").desc(), F.col("desktop_count").desc())
)
