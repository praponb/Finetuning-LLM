from pyspark.sql import functions as F

valid = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.when(F.col("unit_price") >= 380, "High").when(F.col("unit_price") >= 200, "Medium").otherwise("Low").alias("unit_price_level"),
    )
)

result = (
    valid
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 370, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
    )
    .agg(
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("unit_price", 0.5).alias("unit_price_p50"),
    )
    .filter(F.col("unit_price_p50") < 0)
    .orderBy(F.col("unit_price_p50").desc())
)
