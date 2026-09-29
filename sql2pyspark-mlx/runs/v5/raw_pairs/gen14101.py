from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "Central")
    .select("city")
    .unionByName(df_stores.filter(F.col("city") == "London").select("city"))
)
