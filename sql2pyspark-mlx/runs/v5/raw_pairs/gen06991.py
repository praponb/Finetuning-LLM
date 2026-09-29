from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 4)
    .select(
        "id",
        "last_restock",
        "quantity",
        "store_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label_2"),
    )
)
