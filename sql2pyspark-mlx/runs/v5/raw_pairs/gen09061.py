from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["West", "North", "South"]) & (F.upper(F.trim("store_name")) == "SIAM FLAGSHIP"))
    .select("id", "region", "opened_date", "store_name")
    .orderBy(F.col("opened_date").desc())
)
