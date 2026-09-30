from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") > 11545)
    .select("id", "sqft", "city")
    .unionByName(df_stores.filter(F.col("sqft") != 14504).select("id", "sqft", "city"))
)
