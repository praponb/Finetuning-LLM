from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 41, "LARGE").otherwise("SMALL").alias("units_tier"),
    )
    .agg(
        F.stddev_samp("units").alias("units_stddev"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.bit_or("units").alias("units_bit_or"),
    )
)
