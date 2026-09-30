from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2024-09-19")
    .select("id", "city", "store_name")
)
