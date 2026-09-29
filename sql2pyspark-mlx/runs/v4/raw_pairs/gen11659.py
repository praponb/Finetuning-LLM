from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.count("*").alias("num_inventory"),
        F.count("*").alias("cnt"),
    )
)
