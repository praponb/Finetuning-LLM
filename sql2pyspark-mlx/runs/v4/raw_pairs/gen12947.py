from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy("unique_stores")
)
