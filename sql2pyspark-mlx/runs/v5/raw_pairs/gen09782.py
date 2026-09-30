from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["West", "East", "South"]) & (F.col("opened_date") >= "2024-12-01"))
    .select("sqft", "city", "opened_date")
)
