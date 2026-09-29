from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 5) & (F.col("last_restock") > "2024-10-15"))
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.round("quantity", 2).alias("quantity_rounded"),
        F.year("last_restock").alias("last_restock_year"),
    )
)
