from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") == 195)
    .select("last_restock")
    .unionByName(
        df_inventory
        .filter(F.col("product_id").isin([7, 8]))
        .select("last_restock")
    )
)
