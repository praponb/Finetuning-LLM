from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Tokyo")
    .groupBy(F.month("opened_date").alias("opened_month"))
    .pivot("city", ["London", "Bangkok"])
    .agg(F.max("sqft"))
)
