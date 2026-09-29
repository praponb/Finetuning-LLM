from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("order_id")
w_ordered = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id")
w4 = Window.partitionBy("product_id").orderBy("price", "id")
w5 = Window.partitionBy("product_id").orderBy("unit_price", "id")

with_windows = (
    df_order_items
    .filter(F.col("product_id").isin([35, 37, 38]))
    .withColumn("unit_price_avg_2", F.avg("unit_price").over(w))
    .withColumn("quantity_share", F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4))
    .withColumn("unit_price_bucket", F.ntile(10).over(w_ordered))
)

joined = (
    with_windows
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.product_id",
        "e.unit_price",
        F.col("b.product_name"),
        F.col("b.price"),
        F.col("b.stock_qty"),
    )
)

scored = (
    joined
    .select(
        "id",
        "price",
        F.lag("unit_price", 1).over(w4).alias("prev_unit_price"),
        F.lead("unit_price", 1).over(w5).alias("next_unit_price"),
    )
)

filtered = (
    scored
    .filter(F.col("price") <= 950)
    .withColumn("prev_unit_price_whole", F.floor("prev_unit_price"))
)

result = (
    filtered
    .select(
        "id",
        "price",
        "prev_unit_price",
        "next_unit_price",
        "prev_unit_price_whole",
    )
)
