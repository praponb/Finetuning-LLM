from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", F.year("opened_date").alias("opened_year"))
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
    .orderBy("stddev_sqft", "sqft_count")
)
