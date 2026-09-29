from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id") == 3)
    .select("store_id")
    .orderBy(F.col("store_id").desc())
)
