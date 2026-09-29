from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("brand")
w_part = Window.partitionBy("category", "brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("stock_qty", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        F.last_day("launch_date").alias("launch_date_month_end"),
        F.when(F.col("brand").isin(["Initech", "Umbrella"]), "Core").otherwise("Other").alias("brand_group"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "launch_date",
        "launch_date_month_end",
        "brand_group",
        F.corr("stock_qty", "price").over(w_ordered).alias("corr_stock_qty_price"),
        F.first("stock_qty", ignorenulls=True).over(w_part).alias("first_known_stock_qty"),
        F.round((F.col("price") / F.sum("price").over(w_ordered)), 4).alias("price_share"),
        F.corr("price", "stock_qty").over(w_ordered).alias("corr_price_stock_qty"),
        F.lag("price", 1).over(w_recent).alias("prev_price"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.launch_date",
        "s.brand_group",
        "s.first_known_stock_qty",
        "s.price_share",
        "s.corr_price_stock_qty",
        F.col("c.quantity"),
    )
)

with_latest_products = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.launch_date",
        "s.first_known_stock_qty",
        "s.corr_price_stock_qty",
        "s.quantity",
        F.col("c.unit_price"),
    )
)

eligible = (
    with_latest_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity") > 114), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

eligible_products = (
    latest_match
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left_semi")
)

stats = (
    eligible_products
    .cube(
        F.when(F.col("unit_price") >= 380, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("unit_price", 0.5).alias("unit_price_p50"),
        F.count("*").alias("cnt"),
        F.max("unit_price").alias("max_unit_price"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)

result = (
    stats
    .select("week_start", "num_rows", "max_unit_price", "distinct_ids_2")
    .orderBy(F.col("distinct_ids_2").desc())
)
