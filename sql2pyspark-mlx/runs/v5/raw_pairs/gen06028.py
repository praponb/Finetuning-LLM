from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Chiang Mai", "Singapore", "New York"]) & (F.col("city") == "Chiang Mai"))
    .withColumn("sqft_whole", F.floor("sqft"))
)
