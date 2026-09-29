from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .pivot("region", ["South", "East", "West", "North"])
    .agg(F.max("sqft"))
    .orderBy("opened_month")
)
