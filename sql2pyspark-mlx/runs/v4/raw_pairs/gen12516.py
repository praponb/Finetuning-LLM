from pyspark.sql import functions as F

result = (
    df_store_sales
    .rollup(F.date_trunc("year", "sale_date").alias("year_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("units").alias("units_stddev"),
        F.stddev_samp("units").alias("units_stddev_2"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("units_stddev").asc_nulls_first())
)
