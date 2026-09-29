from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") < "2025-03-01") & (F.col("sqft") != 13471))
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
    )
    .filter(F.col("chiang_mai_count") < 39)
)
