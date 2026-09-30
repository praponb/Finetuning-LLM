from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") < 39)
    .agg(F.min("units").alias("min_units"))
    .orderBy(F.col("min_units").desc())
)
