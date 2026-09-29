from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("category", "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("category", "brand")
w5 = Window.partitionBy("brand", "category").orderBy("cost", "id")
w6 = Window.partitionBy("brand").orderBy(F.col("last_restock").desc_nulls_last(), "id")
w7 = Window.partitionBy("brand").orderBy(F.col("running_price").desc(), "id")
w8 = Window.orderBy("row_seq")

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-05-04").cast("date")) & (F.year("launch_date") == 2025))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.upper("brand").alias("brand_upper"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.price",
        "s.cost",
        "s.stock_qty",
        "s.tags",
        "s.brand_upper",
        "s.brand_filled",
        F.col("c.quantity"),
        F.col("c.last_restock"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.year(F.col("x.last_restock")) >= 2025), "left_anti")
)

scored = (
    eligible
    .filter(F.col("order_item_quantity") <= 4)
    .select(
        "id",
        "brand",
        "price",
        "brand_upper",
        "brand_filled",
        "last_restock",
        F.sum("price").over(w3).alias("running_price"),
        F.corr("stock_qty", "cost").over(w4).alias("corr_stock_qty_cost"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    latest
    .withColumn("running_price_bucket", F.ntile(5).over(w7))
    .withColumn("row_seq_rank", F.rank().over(w8))
)

result = (
    metrics
    .groupBy("brand")
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.sum("running_price") / F.nullif(F.count(F.when(F.col("brand") == "Globex", 1)), F.lit(0)), 2).alias("running_price_per_globex"),
    )
)
