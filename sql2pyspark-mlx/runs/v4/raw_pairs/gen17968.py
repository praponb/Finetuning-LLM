from pyspark.sql import functions as F

result = (
    df_stores
    .select("store_name", F.upper("city").alias("city_upper"))
    .orderBy(F.col("city_upper").desc())
)
