from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_last())
w2 = Window.partitionBy("product_id").orderBy(F.col("unit_price_scaled").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price_scaled").asc_nulls_first(), "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_last(), "id")

valid = (
    df_order_items
    .filter(F.col("order_id") == 5)
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "product_id", "unit_price")
)

cleaned = (
    ranked
    .filter(F.col("unit_price") < 110)
    .select("*", F.round((F.col("unit_price") * 1.15), 2).alias("unit_price_scaled"))
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

ranked_order_items = (
    latest
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest_order_items = (
    ranked_order_items
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

enriched = (
    latest_order_items
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .filter(F.size(F.col("d.tags")) > 1)
    .select("s.*", F.col("d.price"), F.col("d.stock_qty"))
)

result = (
    enriched
    .select("id", "product_id", "unit_price", "unit_price_scaled", "price")
    .orderBy(F.col("price").asc_nulls_first())
)
