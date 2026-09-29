from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["West", "East"]))
    .select("store_name")
)
