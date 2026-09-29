from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") == 4713) | F.col("region").isin(["East", "West", "Central"]))
    .select("store_name", "sqft")
)
