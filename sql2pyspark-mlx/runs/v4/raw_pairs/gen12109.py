from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .unionByName(
        df_inventory
        .filter(F.col("last_restock") < "2024-01-01")
        .select(F.col("last_restock").alias("opened_date"))
    )
    .distinct()
)
