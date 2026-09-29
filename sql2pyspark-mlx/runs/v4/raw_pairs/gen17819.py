from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_trunc("YEAR", "last_restock").alias("year_start"))
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids_2"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .orderBy(F.col("distinct_product_ids").asc_nulls_first())
)
