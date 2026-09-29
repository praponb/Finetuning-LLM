from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("category").orderBy("stock_qty", "id")
w_recent = Window.orderBy("seq_no", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-09-05").cast("date")) & (F.col("cost") > 460))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.ceil("stock_qty").alias("stock_qty_whole"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.quantity").between(3, 6)), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") > "2024-04-01"), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.price",
        "s.cost",
        "s.stock_qty",
        "s.tags",
        "s.stock_qty_whole",
        "s.brand_clean",
        F.col("c.units"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

with_windows = (
    latest_match
    .select(
        "id",
        "stock_qty",
        "units",
        F.lead("price").over(w_grp).alias("next_price"),
        F.lag("price").over(w_grp).alias("prev_price"),
        F.lead("stock_qty_whole").over(w_grp).alias("next_stock_qty_whole"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "stock_qty",
        "prev_price",
        "next_stock_qty_whole",
        "seq_no",
        F.lead("next_stock_qty_whole").over(w_recent).alias("next_next_stock_qty_whole"),
        F.lag("next_price").over(w_recent).alias("prev_next_price"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"))
)

result = (
    with_last
    .select(
        "id",
        "next_stock_qty_whole",
        "seq_no",
        "next_next_stock_qty_whole",
        "prev_next_price",
        "row_seq",
        "unit_price",
    )
    .orderBy(F.col("prev_next_price").desc())
)
