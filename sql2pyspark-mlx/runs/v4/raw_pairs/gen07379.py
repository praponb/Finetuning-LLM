from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "West")
    .groupBy("city")
    .agg(F.min("opened_date").alias("first_opened_date"))
    .orderBy("first_opened_date", "city")
    .limit(1)
)
