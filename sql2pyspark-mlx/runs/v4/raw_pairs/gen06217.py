from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") >= "2023-10-11") & (F.col("sqft") >= 12668))
    .select("id", "city", "sqft", F.col("sqft").cast("string").alias("sqft_str"))
    .orderBy("sqft_str", F.col("sqft").desc())
)
