from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("sqft_scaled", F.round((F.col("sqft") * 1.1), 2))
    .withColumn("region_city", F.concat("region", F.lit(", "), "city"))
)
