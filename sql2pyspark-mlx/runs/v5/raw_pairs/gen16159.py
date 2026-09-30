from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") < 69)
    .select("id", "last_restock")
    .unionByName(
        df_inventory
        .filter(F.col("store_id").isin([4, 5, 6]))
        .select("id", "last_restock")
    )
)
