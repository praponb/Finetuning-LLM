from pyspark.sql import functions as F

filtered_stores = df_stores.filter(F.col("store_name").isNotNull())

summary = (
    filtered_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "city")
    .agg(F.min("sqft").alias("min_sqft"))
)

result = summary
