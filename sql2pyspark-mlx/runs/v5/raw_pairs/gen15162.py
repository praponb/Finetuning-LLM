from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("store_id").isin([3, 5]))

product_id_summary = (
    recent_inventory
    .groupBy(
        "product_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
    )
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    product_id_summary
    .filter(F.col("total_quantity") > 1120)
    .select("product_id", "last_restock_month", "total_quantity")
)
