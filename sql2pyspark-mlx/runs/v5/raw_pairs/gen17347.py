from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
        "store_id",
    )
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") > 3)
    .orderBy("last_restock_month")
)
