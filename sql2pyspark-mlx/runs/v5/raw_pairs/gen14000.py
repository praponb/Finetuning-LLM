from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Chiang Mai", "London", "Tokyo"]) & (F.col("sqft") != 15456))
    .select("id", "store_name", F.coalesce("sqft", F.lit(0)).alias("sqft_filled"))
)
