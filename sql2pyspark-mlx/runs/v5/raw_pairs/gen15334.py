from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") > 146)
    .select("id", "quantity")
    .unionByName(
        df_inventory
        .filter(F.month("last_restock").isin([4, 7, 8]))
        .select("id", "quantity")
    )
    .distinct()
)
