from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([3, 7, 8]) & (F.col("sqft") != 13374))
    .select("id", "sqft", "store_name", F.upper("region").alias("region_upper"))
)
