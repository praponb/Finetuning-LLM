from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("city").orderBy("launch_date", "id")
w3 = Window.partitionBy("category").orderBy("cost", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("supplier_id")
w_running = Window.partitionBy("category").orderBy(F.col("prev_price").desc(), "id")

filtered = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-05-11").cast("date"))
    .withColumn("product_name_normalized", F.upper(F.trim("product_name")))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("DIRECT")))
)

joined = (
    filtered
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.rating"), F.col("s.supplier_name"), F.col("s.city"))
)

not_flagged = (
    joined
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isNotNull()), "left_semi")
)

with_windows = (
    not_flagged
    .withColumn("cumulative_stock_qty", F.sum("stock_qty").over(w))
    .withColumn("row_seq", F.row_number().over(w_recent))
    .withColumn("prev_price", F.lag("price", 1).over(w3))
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

metrics = (
    with_latest
    .select(
        "*",
        F.stddev_samp("price").over(w5).alias("supplier_id_stddev_samp_price"),
        F.ntile(10).over(w_running).alias("prev_price_bucket"),
    )
)

with_dims = (
    metrics
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "inner")
    .select(
        "s.*",
        F.col("d.supplier_name").alias("supplier_supplier_name"),
        F.col("d.rating").alias("supplier_rating"),
        F.col("d.city").alias("supplier_city"),
    )
)

result = (
    with_dims
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
    )
    .orderBy("category", "price")
)
