from pyspark.sql import functions as F

result = (
    df_inventory
    .withColumn("week_start", F.date_trunc("WEEK", "last_restock"))
    .cube("week_start")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.grouping("week_start").alias("grouping_id"),
    )
)
