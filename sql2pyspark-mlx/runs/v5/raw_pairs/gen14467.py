from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") >= 15334) & F.col("city").isin(["Singapore", "Berlin"]))
    .select("id", "region", F.lower("store_name").alias("store_name_lower"))
)
