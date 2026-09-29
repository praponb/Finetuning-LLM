from pyspark.sql import functions as F

result = (
    df_orders
    .select("customer_id", F.year("order_date").alias("order_date_year"))
    .orderBy("customer_id", F.col("order_date_year").desc())
)
