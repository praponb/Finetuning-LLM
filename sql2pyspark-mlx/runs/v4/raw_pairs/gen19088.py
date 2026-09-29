from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.orderBy("quantity", "id")
w4 = Window.partitionBy("category", "brand")
w5 = Window.partitionBy("supplier_id", "brand").orderBy("price", "id")
w6 = Window.partitionBy("category").orderBy("cost", "id")
w_recent = Window.partitionBy("category").orderBy("unit_price", "id")

not_flagged = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.units") > 15), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.unit_price").between(120, 260)), "left_anti")
)

latest = (
    eligible
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_latest = (
    latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.unit_price"))
)

result = (
    with_latest
    .select(
        "*",
        F.lag("unit_price", 1).over(w3).alias("prev_unit_price"),
        F.corr("stock_qty", "price").over(w4).alias("corr_stock_qty_price"),
        F.ntile(10).over(w5).alias("price_bucket"),
        F.lag("stock_qty", 1, 0).over(w6).alias("prev_stock_qty"),
        F.ntile(10).over(w_recent).alias("unit_price_bucket"),
    )
)
