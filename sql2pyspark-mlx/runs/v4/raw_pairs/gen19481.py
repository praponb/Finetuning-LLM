from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("s.region").isin(["South", "East"]))
    .groupBy(
        F.when(F.col("s.sqft") >= 12240, "high").when(F.col("s.sqft") >= 6324, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.min(F.col("i.last_restock")).alias("first_last_restock"))
    .orderBy(F.col("sqft_bucket").desc())
)
