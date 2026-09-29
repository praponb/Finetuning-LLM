from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity")
w2 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")

with_windows = (
    df_order_items
    .select(
        "*",
        F.rank().over(w).alias("quantity_rank"),
        F.ntile(10).over(w2).alias("unit_price_bucket"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["product_id", "order_id"], ["product_id"]],
        "product_id",
        "order_id",
    )
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.bit_or("quantity").alias("quantity_bit_or_2"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.percentile_approx("quantity_rank", 0.5).alias("quantity_rank_p50"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price_2"),
    )
    .filter(F.col("corr_quantity_unit_price_2") > 0)
    .orderBy("quantity_bit_or_2")
)
