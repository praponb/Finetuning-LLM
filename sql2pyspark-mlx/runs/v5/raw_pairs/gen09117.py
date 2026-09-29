from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("unique_products") < 2)
)
