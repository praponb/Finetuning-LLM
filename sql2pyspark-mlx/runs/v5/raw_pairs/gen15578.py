from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") < "2025-05-27") | F.col("sqft").isNotNull())
    .agg(F.avg("sqft").alias("avg_sqft"))
    .orderBy(F.col("avg_sqft").desc())
)
