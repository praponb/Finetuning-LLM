from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.max("quantity").alias("max_quantity"),
    )
    .orderBy(F.col("unique_stores").desc())
)
