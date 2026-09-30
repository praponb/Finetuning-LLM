from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
    .orderBy("min_sqft")
)
