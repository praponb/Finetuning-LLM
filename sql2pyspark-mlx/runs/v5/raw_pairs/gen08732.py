from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(F.max("launch_date").alias("last_launch_date"))
)
