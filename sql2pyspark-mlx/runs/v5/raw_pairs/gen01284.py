from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").isNotNull() & (F.col("region").isin(["East", "Central", "North"]) | (F.col("opened_date") >= "2023-05-18")))
    .select("city", "region")
)
