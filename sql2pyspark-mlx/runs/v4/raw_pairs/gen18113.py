from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") > 200)
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
    .filter(F.col("unique_stores") > 9)
    .select("store_id", "quantity_count", "unique_stores")
    .orderBy("unique_stores")
)
