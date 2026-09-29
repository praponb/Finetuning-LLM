from pyspark.sql import functions as F

result = (
    df_inventory
    .select("store_id", "product_id")
    .subtract(
        df_inventory
        .filter(F.year("last_restock") >= 2023)
        .select("store_id", "product_id")
    )
)
