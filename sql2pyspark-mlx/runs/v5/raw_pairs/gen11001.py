from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy("stock_qty", "id")
w3 = Window.partitionBy("category").orderBy("row_seq", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("category").orderBy("order_item_unit_price", "id")
w5 = Window.partitionBy("category").orderBy(F.col("next_stock_qty_plus_price").desc(), "id")
w6 = Window.partitionBy("category").orderBy("prev_stock_qty_plus_price", "id").rowsBetween(-2, -1)
w7 = Window.orderBy("next_order_item_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter(~F.col("brand").isin(["Globex", "Umbrella", "Acme"]))
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        (F.col("stock_qty") + F.col("price")).alias("stock_qty_plus_price"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.stock_qty",
        "s.launch_date",
        "s.tags",
        "s.stock_qty_plus_price",
        "s.brand_clean",
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

metrics = (
    with_latest
    .withColumn("next_order_item_unit_price", F.lead("order_item_unit_price").over(w2))
    .withColumn("prev_order_item_unit_price", F.lag("order_item_unit_price").over(w2))
    .withColumn("next_stock_qty_plus_price", F.lead("stock_qty_plus_price").over(w2))
    .withColumn("prev_stock_qty_plus_price", F.lag("stock_qty_plus_price").over(w2))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("stock_qty_bucket", F.ntile(3).over(w2))
)

scored = (
    metrics
    .withColumn("stock_qty_avg_2_prior", F.avg("stock_qty").over(w3))
    .withColumn("prev_next_order_item_unit_price", F.lag("next_order_item_unit_price", 2).over(w4))
    .withColumn("row_seq_2", F.row_number().over(w5))
    .withColumn("next_stock_qty_plus_price_avg_2_prior", F.avg("next_stock_qty_plus_price").over(w6))
    .withColumn("last_known_row_seq", F.last("row_seq", ignorenulls=True).over(w7))
)

with_last = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.stock_qty",
        "s.launch_date",
        "s.tags",
        "s.stock_qty_plus_price",
        "s.next_order_item_unit_price",
        "s.next_stock_qty_plus_price",
        "s.stock_qty_bucket",
        "s.stock_qty_avg_2_prior",
        "s.row_seq_2",
        "s.next_stock_qty_plus_price_avg_2_prior",
        F.col("c.last_restock"),
    )
)

report = (
    with_last
    .select(
        "id",
        "category",
        "stock_qty",
        "launch_date",
        "tags",
        "next_order_item_unit_price",
        "next_stock_qty_plus_price",
        "stock_qty_bucket",
        "stock_qty_avg_2_prior",
    )
    .orderBy(
        F.col("next_order_item_unit_price").desc(),
        F.col("next_stock_qty_plus_price").desc(),
    )
)

result = (
    report
    .select(
        "category",
        "stock_qty",
        "next_order_item_unit_price",
        "next_stock_qty_plus_price",
        "stock_qty_bucket",
        "stock_qty_avg_2_prior",
    )
    .orderBy(F.col("next_order_item_unit_price").desc())
)
