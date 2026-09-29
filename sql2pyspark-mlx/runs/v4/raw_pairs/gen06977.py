from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")
w2 = Window.partitionBy("brand").orderBy("stock_qty", "id")

valid = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("supplier_id") == 15))
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
        "supplier_id",
        F.substring("category", 1, 4).alias("category_prefix"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id").isin([1, 12])), "left_semi")
)

latest = (
    not_flagged
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

valid_products = (
    latest
    .filter((F.col("launch_date") > F.lit("2024-03-26").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "category_prefix",
        (F.col("price") / F.nullif("stock_qty", F.lit(0))).alias("price_per_stock_qty"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

with_windows = (
    valid_products
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.ntile(3).over(w2).alias("stock_qty_bucket"),
    )
)

cleaned = (
    with_windows
    .filter((F.col("launch_date") > F.lit("2023-04-20").cast("date")) & F.col("brand").isNotNull() & (F.col("supplier_id") == 40))
    .withColumn("category_prefix_2", F.substring("category", 1, 4))
)

totals = (
    cleaned
    .cube("category", F.date_trunc("MONTH", "launch_date").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_rows"),
        F.corr("stock_qty", "price").alias("corr_stock_qty_price"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_rows"),
    )
)

result = (
    totals
    .select("umbrella_rows", "corr_stock_qty_price", "globex_rows")
    .orderBy(F.col("umbrella_rows").desc_nulls_first())
)
