from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("max_quantity") > 190)
)
