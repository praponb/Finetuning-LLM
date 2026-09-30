from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 350, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
    )
    .agg(
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
    .filter(F.col("quantity_bit_or") > 1)
)
