from pyspark.sql import functions as F

result = (
    df_orders
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.country").alias("country"), F.col("e.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.discount")).alias("max_discount"),
    )
)
