from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 12365, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
    )
    .agg(
        F.count(F.when(F.col("region") == "North", 1)).alias("north_rows"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("region") == "Central", F.col("sqft")).otherwise(0)).alias("central_sqft"),
    )
    .orderBy(F.col("central_sqft").desc())
)
