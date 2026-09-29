from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(19, 31))
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
    .filter(F.col("stddev_units") > 68)
    .orderBy("stddev_units")
)
