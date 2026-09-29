from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~(F.col("sqft").between(7132, 12280) & (F.col("region") == "East")))
    .select("sqft", "store_name", F.col("sqft").cast("double").alias("sqft_num"))
)
