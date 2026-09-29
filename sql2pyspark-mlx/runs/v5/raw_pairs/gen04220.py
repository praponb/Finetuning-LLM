from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .agg(F.count("*").alias("cnt"))
)
