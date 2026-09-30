from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") == 36)
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
)
