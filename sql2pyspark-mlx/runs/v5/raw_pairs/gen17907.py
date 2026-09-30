from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.col("sqft") != 15772)

city_summary = (
    valid_stores
    .groupBy("city", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.sum("sqft").alias("total_sqft"))
)

result = city_summary.orderBy("opened_month")
