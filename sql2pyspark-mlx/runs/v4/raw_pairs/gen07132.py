from pyspark.sql import functions as F

result = (
    df_inventory
    .rollup(
        F.when(F.col("quantity") >= 211, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        "store_id",
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.max("quantity").alias("max_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .orderBy(F.col("avg_quantity").asc_nulls_last())
)
