from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .pivot("region", ["North", "West", "Central"])
    .agg(F.avg("sqft"))
    .orderBy("opened_month")
)
