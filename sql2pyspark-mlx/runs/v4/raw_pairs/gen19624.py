from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("unit_price_stddev") > 0)
    .orderBy("quantity_tier")
)
