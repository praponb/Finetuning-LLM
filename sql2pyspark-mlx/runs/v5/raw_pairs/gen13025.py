from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "city")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.max("opened_date").alias("last_opened_date"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") > 23)
    .orderBy("city")
)
