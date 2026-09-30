from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "store_name", F.round((F.col("sqft") * 1.1), 2).alias("sqft_scaled"))
    .orderBy(F.col("store_name").desc())
)
