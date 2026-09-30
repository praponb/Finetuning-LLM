from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNotNull())
    .groupBy("city")
    .agg(
        F.count("*").alias("cnt"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") > 6800)
)
