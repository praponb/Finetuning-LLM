from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"))
    .filter(F.col("chiang_mai_count") > 4)
    .select("city", "chiang_mai_count")
    .orderBy("city")
)
