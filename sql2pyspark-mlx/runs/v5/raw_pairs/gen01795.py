from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("stock_qty", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("launch_date", "id")
w4 = Window.partitionBy("store_id").orderBy("stock_qty", "id")
w5 = Window.partitionBy("store_id").orderBy("stock_qty_bucket", "id")

enriched = (
    df_inventory
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id").isin([8, 40]))
    .select(
        "src.*",
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "store_id",
        "launch_date",
        "stock_qty",
        F.avg("quantity").over(w).alias("quantity_avg_3"),
        F.last("quantity", ignorenulls=True).over(w2).alias("last_known_quantity"),
    )
)

scored_inventory = (
    scored
    .select(
        "id",
        "store_id",
        "stock_qty",
        "quantity_avg_3",
        "last_known_quantity",
        F.lag("last_known_quantity").over(w3).alias("prev_last_known_quantity"),
        F.lag("quantity_avg_3").over(w3).alias("prev_quantity_avg_3"),
        F.row_number().over(w3).alias("event_seq"),
        F.ntile(10).over(w4).alias("stock_qty_bucket"),
    )
)

cleaned = (
    scored_inventory
    .filter(F.col("quantity_avg_3") == 203)
    .select(
        "id",
        "store_id",
        "stock_qty",
        "event_seq",
        "stock_qty_bucket",
        (F.col("prev_last_known_quantity") - F.col("prev_quantity_avg_3")).alias("prev_last_known_quantity_minus_prev_quantity_avg_3"),
        F.round("stock_qty", 0).alias("stock_qty_rounded"),
    )
)

enriched_inventory = (
    cleaned
    .select(
        "id",
        "stock_qty",
        "event_seq",
        "stock_qty_rounded",
        F.lead("stock_qty_rounded").over(w5).alias("next_stock_qty_rounded"),
        F.lead("stock_qty").over(w5).alias("next_stock_qty"),
        F.lag("stock_qty").over(w5).alias("prev_stock_qty"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

base = (
    enriched_inventory
    .filter(F.col("next_stock_qty") == 387)
    .withColumn("next_stock_qty_rounded_whole", F.floor("next_stock_qty_rounded"))
)

result = (
    base
    .select("id", "stock_qty_rounded", "row_seq")
    .orderBy(F.col("row_seq").desc())
)
