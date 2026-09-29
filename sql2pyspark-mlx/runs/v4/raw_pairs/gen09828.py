from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["COMPLETED", "CANCELLED"]))
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.sum("discount").alias("total_discount"),
        F.count("discount").alias("discount_count"),
        F.count("*").alias("num_orders"),
    )
    .filter(F.col("total_discount") < 0.9)
)
