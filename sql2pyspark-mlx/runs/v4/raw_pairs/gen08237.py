from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 340, "HIGH").otherwise("LOW").alias("unit_price_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
)
