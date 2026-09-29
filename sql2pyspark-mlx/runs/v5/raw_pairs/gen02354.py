from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 4)
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(F.count("discount").alias("discount_count"))
    .filter(F.col("discount_count") < 5)
    .orderBy(F.col("discount_count").desc(), "order_year")
    .limit(10)
)
