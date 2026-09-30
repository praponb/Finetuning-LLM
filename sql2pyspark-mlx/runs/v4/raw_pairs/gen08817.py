from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .pivot("region", ["West", "Central", "South", "East"])
    .agg(F.max("sqft"))
)
