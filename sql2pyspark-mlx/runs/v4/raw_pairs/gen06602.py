from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .pivot("city", ["Bangkok", "Singapore"])
    .agg(F.sum("sqft"))
    .orderBy("opened_quarter")
)
