from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["South", "Central"]) & (F.col("store_name").isNull() | (F.col("city") == "Berlin")))
    .select("id", "store_name", "sqft")
    .orderBy(F.col("id").desc())
    .limit(5)
)
