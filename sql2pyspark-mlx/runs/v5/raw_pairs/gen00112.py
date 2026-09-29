from pyspark.sql import functions as F

result = (
    df_orders
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["city", "status"], ["city"], []],
        F.col("p.city").alias("city"),
        F.col("e.status").alias("status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.discount")).alias("max_discount"),
    )
)
