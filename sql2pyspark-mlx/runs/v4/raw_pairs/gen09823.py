from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)
w_ordered = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_order_items
    .select(
        "*",
        F.avg("quantity").over(w).alias("quantity_avg_6"),
        F.sum("quantity").over(w_ordered).alias("cumulative_quantity"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("quantity") >= 7, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        F.when(F.col("quantity_avg_6") >= 9, "HIGH").otherwise("LOW").alias("quantity_avg_6_tier"),
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
    )
    .orderBy(F.col("quantity_tier").desc())
)
