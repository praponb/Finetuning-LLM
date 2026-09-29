from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["South", "West", "Central"]))
    .select("id", "sqft")
    .intersect(df_stores.filter(F.col("city") != "Chiang Mai").select("id", "sqft"))
)
