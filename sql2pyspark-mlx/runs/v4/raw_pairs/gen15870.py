from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.round(F.avg("sqft"), 2).alias("avg_sqft"))
)
