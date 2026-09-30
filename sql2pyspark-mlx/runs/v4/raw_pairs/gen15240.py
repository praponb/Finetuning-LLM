from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Kiosk%"))
    .select("id", "store_name")
)
