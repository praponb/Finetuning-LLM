from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2023-06-08", "2025-05-07") & (F.col("order_date") <= "2023-08-07"))
    .groupBy(
        F.quarter("order_date").alias("order_quarter"),
        F.date_format("order_date", "yyyy-MM").alias("order_month"),
    )
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") < 490)
    .orderBy(F.col("order_quarter").desc(), "order_month")
    .limit(5)
)
