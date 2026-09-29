from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") != 10816) & ((F.col("sqft") <= 15646) | F.col("store_name").isNotNull()))
    .select("id", "opened_date", "sqft", "region")
)
