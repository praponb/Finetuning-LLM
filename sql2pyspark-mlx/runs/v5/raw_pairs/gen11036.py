from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("stock_qty_times_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("stock_qty", "id")
w3 = Window.partitionBy("brand", "category").orderBy(F.col("stock_qty").asc_nulls_first())
w4 = Window.partitionBy("category").orderBy("stock_qty_times_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("brand").orderBy("stock_qty_times_price", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

valid = (
    df_products
    .filter(F.col("brand").isNotNull())
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
        (F.col("stock_qty") * F.col("price")).alias("stock_qty_times_price"),
    )
)

with_windows = (
    valid
    .withColumn("running_price", F.sum("price").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("stock_qty_rank", F.rank().over(w3))
    .withColumn("first_known_stock_qty_times_price", F.first("stock_qty_times_price", ignorenulls=True).over(w4))
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w_hist))
)

metrics = (
    with_windows
    .select(
        "id",
        "product_name",
        "cost",
        "running_price",
        "row_seq",
        "stock_qty_rank",
        F.max("running_price").over(w6).alias("last4_max_running_price"),
        F.sum("running_price").over(w6).alias("last4_sum_running_price"),
        F.min("running_price").over(w6).alias("last4_min_running_price"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.cost",
        "s.running_price",
        "s.row_seq",
        "s.stock_qty_rank",
        "s.last4_max_running_price",
        "s.last4_sum_running_price",
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

latest_match_products = (
    latest_match
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.last_restock"))
)

with_last = (
    latest_match_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock_2"),
    )
)

with_latest = (
    with_last
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.last4_max_running_price",
        "s.inventory_last_restock",
        "s.quantity",
        "s.inventory_quantity",
        F.col("c.quantity").alias("inventory_quantity_2"),
        F.col("c.last_restock").alias("inventory_last_restock_3"),
    )
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("last4_max_running_price") >= 6500, "LARGE").otherwise("SMALL").alias("last4_max_running_price_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("inventory_quantity"), 2).alias("avg_inventory_quantity"),
        F.percentile_approx("inventory_quantity_2", [0.25, 0.5, 0.75]).alias("inventory_quantity_2_quartiles"),
    )
)
