from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") > 10540)
    .select("sqft", "region", F.upper("region").alias("region_upper"))
    .orderBy("region", F.col("region_upper").desc())
)
