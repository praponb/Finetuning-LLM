from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["West", "Central"]))
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.count("*").alias("row_count"),
        F.count("*").alias("store_count"),
    )
)
