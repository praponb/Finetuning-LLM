from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["Central", "East"]))
    .groupBy("region", "city")
    .agg(
        F.countDistinct("sqft").alias("unique_citys"),
        F.count("*").alias("row_count"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy("region", "min_opened_date", "city")
    .limit(50)
)
