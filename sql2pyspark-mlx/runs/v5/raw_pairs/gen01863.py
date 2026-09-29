from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("store_name").like("%Old%") & (F.col("city") != "Berlin")) | F.col("city").isin(["Chiang Mai", "Bangkok"]))
    .groupBy("region")
    .agg(F.count("*").alias("row_count"))
)
