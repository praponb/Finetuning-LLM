from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "London") & (F.col("sqft") > 12841))
    .rollup("region", "city")
    .agg(F.count("*").alias("num_stores"))
)
