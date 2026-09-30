from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.orderBy("cost", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("category")
w5 = Window.partitionBy("category").orderBy("order_item_quantity", "id")
w6 = Window.partitionBy("category").orderBy("stock_qty_avg_6_prior", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.orderBy("cost", "id").rowsBetween(-5, -1)
w8 = Window.orderBy(F.col("cost").desc(), "id")
w9 = Window.partitionBy("category").orderBy("stock_qty_avg_6_prior", "id")
w10 = Window.partitionBy("category").orderBy(F.col("stock_qty_avg_6_prior").desc(), "id")

with_last = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

latest_match = (
    with_last
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.price",
        "s.cost",
        "s.stock_qty",
        "s.launch_date",
        "s.inventory_last_restock",
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

with_windows = (
    latest_match
    .filter(F.col("launch_date") < "2023-05-01")
    .select(
        "id",
        "category",
        "cost",
        "stock_qty",
        "launch_date",
        F.avg("stock_qty").over(w3).alias("stock_qty_avg_6_prior"),
        F.min("cost").over(w4).alias("category_min_cost"),
        F.round((F.col("order_item_quantity") / F.sum("order_item_quantity").over(w4)), 4).alias("order_item_quantity_share"),
        F.lead("stock_qty", 1).over(w5).alias("next_stock_qty"),
    )
)

with_windows_products = (
    with_windows
    .select(
        "id",
        "stock_qty",
        "launch_date",
        "stock_qty_avg_6_prior",
        F.avg("order_item_quantity_share").over(w6).alias("order_item_quantity_share_avg_6"),
        F.avg("cost").over(w7).alias("cost_avg_5_prior"),
        F.row_number().over(w8).alias("row_seq"),
        F.lag("next_stock_qty", 1).over(w9).alias("prev_next_stock_qty"),
        F.row_number().over(w10).alias("seq_no"),
    )
)

base = (
    with_windows_products
    .filter((F.col("launch_date") > F.lit("2023-05-21").cast("date")) & (F.col("launch_date") > "2024-07-18"))
    .select(
        "*",
        F.round((F.col("prev_next_stock_qty") * 1.15), 2).alias("prev_next_stock_qty_adjusted"),
        F.add_months("launch_date", 3).alias("launch_date_plus_months"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

base_products = (
    with_latest
    .filter(F.col("launch_date").between("2023-09-01", "2024-11-01"))
    .select(
        "id",
        "launch_date",
        "stock_qty_avg_6_prior",
        "order_item_quantity_share_avg_6",
        "cost_avg_5_prior",
        "row_seq",
        "seq_no",
        "prev_next_stock_qty_adjusted",
        "quantity",
        (F.col("stock_qty") * F.col("order_item_quantity_share_avg_6")).alias("stock_qty_times_order_item_quantity_share_avg_6"),
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
    )
)

result = (
    base_products
    .select(
        "id",
        "stock_qty_times_order_item_quantity_share_avg_6",
        "stock_qty_filled",
        "prev_next_stock_qty_adjusted",
    )
    .unpivot(
        ids=["id"],
        values=["stock_qty_times_order_item_quantity_share_avg_6", "stock_qty_filled", "prev_next_stock_qty_adjusted"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
