from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["London", "Singapore"]) & (F.length("store_name") >= 13))
    .select("id", "sqft", F.round("sqft", 0).alias("sqft_rounded"))
)
