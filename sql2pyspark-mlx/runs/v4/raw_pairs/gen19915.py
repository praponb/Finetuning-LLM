from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "Central")
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.count("sqft").alias("sqft_count"))
    .orderBy(F.col("sqft_count").desc())
)
