from pyspark.sql import functions as F

result = (
    df_order_items
    .rollup(
        F.when(F.col("unit_price") >= 320, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.countDistinct("id").alias("distinct_ids"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.count("*").alias("event_count"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .orderBy("corr_quantity_unit_price")
)
