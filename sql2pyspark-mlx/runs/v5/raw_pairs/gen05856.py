from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Berlin") & (F.col("opened_date") < "2025-09-25"))
    .withColumn("clean_region", F.trim("region"))
    .withColumn("adjusted_sqft", F.round((F.col("sqft") * 1.07), 2))
)
