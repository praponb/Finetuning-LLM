from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(~F.col("s.region").isin(["East", "South"]))
    .select(
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("i.quantity"),
        F.col("i.product_id"),
    )
    .orderBy("sqft", F.col("store_name").desc())
)
