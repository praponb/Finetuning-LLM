from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.sum("quantity").alias("total_quantity"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.stddev_samp("quantity").alias("quantity_stddev_2"),
    )
)
