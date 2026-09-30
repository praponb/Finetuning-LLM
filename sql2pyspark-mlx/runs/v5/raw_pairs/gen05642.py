from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Tokyo") & F.col("region").isNotNull())
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.max("sqft").alias("max_sqft"),
    )
)
