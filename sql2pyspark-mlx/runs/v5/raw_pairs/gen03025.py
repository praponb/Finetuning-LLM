from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").isNotNull())
    .select(
        "id",
        "store_id",
        F.round("quantity", 0).alias("quantity_rounded"),
        F.floor("quantity").alias("quantity_whole"),
    )
    .orderBy(F.col("quantity_rounded").desc(), F.col("quantity_whole").desc())
)
