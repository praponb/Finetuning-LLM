from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") > 6116)
    .select("region")
    .unionByName(df_stores.filter(F.col("sqft") == 14510).select("region"))
    .distinct()
    .orderBy("region")
)
