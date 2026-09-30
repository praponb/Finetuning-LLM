from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category", "supplier_id").orderBy("price_per_stock_qty", "id")
w3 = Window.orderBy("price_per_stock_qty", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("category").orderBy("price_per_stock_qty_bucket", "id")
w5 = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")
w6 = Window.partitionBy("category")

valid = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-07-26").cast("date"))
    .select(
        "id",
        "category",
        "price",
        "stock_qty",
        "launch_date",
        "supplier_id",
        (F.col("price") / F.nullif("stock_qty", F.lit(0))).alias("price_per_stock_qty"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.first("price_per_stock_qty", ignorenulls=True).over(w).alias("first_known_price_per_stock_qty"),
        F.ntile(10).over(w2).alias("price_per_stock_qty_bucket"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left_semi")
)

scored_products = (
    not_flagged
    .select(
        "*",
        F.avg("price_per_stock_qty").over(w3).alias("price_per_stock_qty_avg_5"),
        F.lag("price", 2).over(w4).alias("prev_price"),
        F.ntile(3).over(w5).alias("price_bucket"),
        F.corr("stock_qty", "price").over(w6).alias("corr_stock_qty_price"),
    )
)

result = (
    scored_products
    .select(
        "id",
        "category",
        "price",
        "launch_date",
        "price_per_stock_qty",
        "price_per_stock_qty_bucket",
        "price_per_stock_qty_avg_5",
        "prev_price",
        "price_bucket",
        F.when(F.col("category").rlike("^(Fashion|Books)$"), "CORE").when(F.col("category").rlike("^(Electronics|Home)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("category_zone"),
    )
    .orderBy(F.col("category_zone").desc_nulls_first(), "price_per_stock_qty")
)
