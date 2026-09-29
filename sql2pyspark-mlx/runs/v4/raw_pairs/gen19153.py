from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([1, 2, 4]))
    .select("id", "product_id", "last_restock")
    .unionByName(
        df_inventory
        .filter(F.col("product_id").isin([1, 6]))
        .select("id", "product_id", "last_restock")
    )
    .distinct()
)
