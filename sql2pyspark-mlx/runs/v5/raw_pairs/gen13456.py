from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").isNotNull())
    .groupBy("city")
    .agg(F.min("opened_date").alias("first_opened_date"))
    .orderBy("city")
)
