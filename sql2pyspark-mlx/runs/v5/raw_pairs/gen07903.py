from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") > 1450) | F.col("discount").between(0.33, 0.34))
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.count("*").alias("order_count"),
        F.count("*").alias("num_orders"),
    )
    .filter(F.col("order_count") < 5)
)
