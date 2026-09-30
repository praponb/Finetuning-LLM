from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNotNull())
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
    .filter(F.col("stddev_sqft") > 70)
    .select("opened_month", "stddev_sqft")
    .orderBy(F.col("opened_month").desc())
)
