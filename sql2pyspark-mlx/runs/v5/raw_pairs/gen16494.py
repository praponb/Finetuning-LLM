from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "North")
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.sum("sqft").alias("total_sqft"))
    .orderBy("opened_month")
)
