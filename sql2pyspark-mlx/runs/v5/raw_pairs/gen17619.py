from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 15593, "LARGE").otherwise("SMALL").alias("sqft_tier"),
        "region",
    )
    .agg(
        F.sum(F.when(F.col("region") == "North", F.col("sqft")).otherwise(0)).alias("north_sqft"),
        F.count("*").alias("cnt"),
    )
    .orderBy("region")
)
