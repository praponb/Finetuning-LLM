from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "store_id",
        F.add_months("last_restock", 1).alias("last_restock_plus_months"),
        F.round((F.col("quantity") * 1.15), 2).alias("quantity_adjusted"),
    )
)
