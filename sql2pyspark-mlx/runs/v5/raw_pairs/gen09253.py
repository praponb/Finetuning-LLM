from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 5, 6]))
    .select("id", "quantity")
    .intersect(
        df_inventory
        .filter(F.month("last_restock").isin([2, 6, 10]))
        .select("id", "quantity")
    )
    .orderBy(F.col("id").desc())
)
