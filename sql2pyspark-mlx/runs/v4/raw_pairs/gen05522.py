from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 4000)
    .select("store_id")
    .unionByName(df_store_sales.filter(F.col("store_id") == 3).select("store_id"))
    .distinct()
    .orderBy("store_id")
)
