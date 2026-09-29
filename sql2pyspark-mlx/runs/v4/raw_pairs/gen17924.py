from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("S%"))
    .select("region")
    .subtract(df_stores.filter(F.col("store_name").like("%Sia%")).select("region"))
)
