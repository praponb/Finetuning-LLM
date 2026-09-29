from pyspark.sql import functions as F

result = (
    df_orders
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .groupingSets(
        [["segment", "status"], ["segment"], []],
        F.col("c.segment").alias("segment"),
        F.col("src.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("src.discount")), 2).alias("total_discount"),
        F.round(F.avg(F.col("src.amount")), 2).alias("avg_amount"),
    )
)
