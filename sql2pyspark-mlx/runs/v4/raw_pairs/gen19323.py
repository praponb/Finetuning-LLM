from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "West") | (F.col("opened_date") <= "2025-04-01"))
    .agg(F.count("sqft").alias("sqft_count"))
)
