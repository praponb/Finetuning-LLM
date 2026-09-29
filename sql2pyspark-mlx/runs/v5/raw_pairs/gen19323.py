from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.orderBy("quantity", "id")

metrics = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.lead("quantity", 1).over(w_recent).alias("next_quantity"),
        F.ntile(3).over(w2).alias("quantity_bucket"),
        F.lag("quantity", 1).over(w3).alias("prev_quantity"),
    )
)

result = (
    metrics
    .groupBy(
        "product_id",
        F.when(F.col("quantity_bucket") >= 9, "HIGH").otherwise("LOW").alias("quantity_bucket_tier"),
    )
    .agg(
        F.percentile_approx("next_quantity", [0.25, 0.5, 0.75]).alias("next_quantity_quartiles"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.percentile_approx("prev_quantity", [0.25, 0.5, 0.75]).alias("prev_quantity_quartiles"),
    )
    .orderBy("product_id")
)
