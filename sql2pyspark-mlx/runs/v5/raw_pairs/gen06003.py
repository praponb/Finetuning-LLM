from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
    .withColumn("sqft_rounded", F.round("sqft", 2))
    .orderBy(F.col("sqft").desc())
)
