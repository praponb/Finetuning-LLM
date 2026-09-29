from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 390, "PREMIUM").otherwise("STANDARD").alias("unit_price_tier"),
    )
    .agg(
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
    )
    .orderBy(F.col("unit_price_tier").desc_nulls_last())
)
