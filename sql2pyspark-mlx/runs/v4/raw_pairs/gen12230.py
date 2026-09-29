from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-05-09", "2025-01-12"))
    .groupBy("city")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
    .filter(F.col("avg_sqft") >= 11600)
    .orderBy("unique_citys", "city")
)
