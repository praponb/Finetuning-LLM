from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").isNull())
    .select(
        "id",
        "store_id",
        F.add_months("last_restock", 12).alias("last_restock_plus_months"),
        F.col("quantity").cast("double").alias("quantity_str"),
    )
    .orderBy("store_id", "id")
    .limit(3)
)
