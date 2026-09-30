from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(F.count("discount").alias("discount_count"))
    .filter(F.col("discount_count") < 8)
    .select("order_quarter", "discount_count")
)
