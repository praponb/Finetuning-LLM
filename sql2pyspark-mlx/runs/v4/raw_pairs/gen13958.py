from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 84)
    .groupBy(
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
        "store_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("quantity").alias("quantity_count"),
    )
)
