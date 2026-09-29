from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price_2"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
    .filter(F.col("avg_unit_price_2") < 0)
)
