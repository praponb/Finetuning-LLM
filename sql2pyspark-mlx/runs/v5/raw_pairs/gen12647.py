from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 34, "LARGE").otherwise("SMALL").alias("units_tier"),
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.bit_or("units").alias("units_bit_or"),
        F.countDistinct("product_id").alias("distinct_product_ids_2"),
        F.stddev_samp("units").alias("units_stddev"),
        F.bit_and("units").alias("units_bit_and"),
    )
    .orderBy(F.col("units_bit_or").asc_nulls_last())
)
