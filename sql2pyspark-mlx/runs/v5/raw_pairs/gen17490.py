from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    df_stores.alias("stores")
    .join(store_totals.alias("store_totals"), F.col("stores.id") == F.col("store_totals.store_id"), "left")
    .select(
        F.col("stores.id"),
        F.col("stores.sqft"),
        F.col("store_totals.min_quantity"),
    )
    .orderBy(F.col("sqft").desc())
)
