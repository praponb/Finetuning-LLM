from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 340, "HIGH").otherwise("LOW").alias("unit_price_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
    )
    .orderBy(F.col("corr_unit_price_quantity").asc_nulls_last())
)
