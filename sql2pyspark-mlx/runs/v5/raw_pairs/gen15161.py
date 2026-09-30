from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.min("opened_date").alias("first_opened_date"),
        F.avg("sqft").alias("avg_sqft"),
        F.max("sqft").alias("max_sqft"),
    )
    .filter(F.col("max_sqft") > 9000)
    .drop("max_sqft")
    .orderBy("opened_quarter")
    .limit(50)
)
