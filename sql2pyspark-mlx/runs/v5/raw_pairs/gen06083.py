from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 4)
    .groupBy("product_id", "store_id")
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
    .filter(F.col("stddev_units") < 71)
    .orderBy(F.col("store_id").desc(), "product_id")
)
