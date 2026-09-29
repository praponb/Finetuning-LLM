from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 1) & (F.col("product_id") == 8))
    .select(
        "id",
        "last_restock",
        "quantity",
        "product_id",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
    )
    .orderBy(F.col("id").desc())
)
