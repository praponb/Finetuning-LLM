from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "UK", "DE"]) & (F.col("city") == "New York"))
    .agg(F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"))
    .orderBy("chiang_mai_count")
)
