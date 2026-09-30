from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Chiang Mai")
    .withColumn("sqft_filled", F.coalesce("sqft", F.lit(0)))
    .orderBy("region")
)
