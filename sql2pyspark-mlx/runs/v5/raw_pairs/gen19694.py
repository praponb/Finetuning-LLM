from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "South")
    .select("id", "region", "store_name", "opened_date")
    .orderBy(F.col("id").desc())
    .limit(50)
)
