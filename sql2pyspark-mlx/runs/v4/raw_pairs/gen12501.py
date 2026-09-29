from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")
w2 = Window.orderBy("unit_price", "id")
w3 = Window.partitionBy("product_id")

with_windows = (
    df_order_items
    .withColumn("next_quantity", F.lead("quantity", 2).over(w))
    .withColumn("next_unit_price", F.lead("unit_price", 1).over(w2))
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w3))
)

result = (
    with_windows
    .groupBy("order_id")
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.percentile_approx("next_unit_price", [0.25, 0.5, 0.75]).alias("next_unit_price_quartiles"),
    )
    .filter(F.col("quantity_bit_and") >= 1)
    .orderBy(F.col("distinct_order_ids").desc_nulls_last())
)
