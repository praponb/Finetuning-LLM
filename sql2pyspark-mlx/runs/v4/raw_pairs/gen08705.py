from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(F.month("opened_date").isin([2, 4, 9]) | F.col("sqft").between(8468, 12490))
)

region_summary = (
    filtered_stores
    .groupBy("region")
    .agg(F.sum("sqft").alias("total_sqft"))
)

result = (
    region_summary
    .filter(F.col("total_sqft") < 76000)
    .select("region", "total_sqft")
    .orderBy(F.col("total_sqft").desc())
)
