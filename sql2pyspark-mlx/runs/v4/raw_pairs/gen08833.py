from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(((F.col("city") == "Tokyo") & F.col("city").isin(["Singapore", "Chiang Mai"])) | (F.col("city") == "Bangkok"))
)

stores_stats = (
    filtered_stores
    .groupBy("region")
    .agg(F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"))
)

result = stores_stats.select("region", "chiang_mai_count")
