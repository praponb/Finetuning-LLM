from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .unionByName(df_orders.select(F.col("order_date").alias("launch_date")))
    .distinct()
    .orderBy(F.col("launch_date").desc())
)
