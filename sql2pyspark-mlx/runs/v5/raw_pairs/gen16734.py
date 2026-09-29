from pyspark.sql import functions as F

result = (
    df_inventory
    .select("product_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") < "2024-03-02")
        .select("product_id", "id")
    )
)
