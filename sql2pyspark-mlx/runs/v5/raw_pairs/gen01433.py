from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.bit_or("units").alias("units_bit_or"),
        F.stddev_samp("units").alias("units_stddev"),
        F.bit_or("units").alias("units_bit_or_2"),
        F.bit_and("units").alias("units_bit_and"),
    )
)
