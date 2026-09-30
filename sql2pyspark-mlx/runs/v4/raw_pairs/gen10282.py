from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.year("last_restock") == 2025) & F.month("last_restock").isin([2, 5, 10]))
    .select(
        "id",
        "store_id",
        "quantity",
        "product_id",
        F.add_months("last_restock", 6).alias("last_restock_plus_months"),
    )
)
