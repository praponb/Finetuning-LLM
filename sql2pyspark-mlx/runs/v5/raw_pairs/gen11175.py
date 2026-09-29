from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("stock_qty_minus_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy("cost", "id")
w3 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")
w4 = Window.partitionBy("category", "supplier_id")
w_hist = Window.partitionBy("category")
w6 = Window.partitionBy("category").orderBy(F.col("cumulative_stock_qty").desc_nulls_last())
w7 = Window.orderBy(F.col("stock_qty").desc())

base = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-03-02").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        (F.col("stock_qty") - F.col("price")).alias("stock_qty_minus_price"),
        F.regexp_replace("product_name", " ", "-").alias("product_name_slug"),
    )
)

with_windows = (
    base
    .filter(F.col("cost") > 510)
    .select(
        "*",
        F.sum("stock_qty").over(w).alias("cumulative_stock_qty"),
        F.lead("stock_qty", 1).over(w2).alias("next_stock_qty"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

kept_keys = (
    with_windows
    .select("id", "category")
    .subtract(
        df_products
        .filter(F.col("product_name").like("Noi%"))
        .select("id", "category")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

with_windows_products = (
    reconciled
    .select(
        "id",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "cumulative_stock_qty",
        "row_seq",
        F.stddev_samp("stock_qty").over(w4).alias("category_stddev_samp_stock_qty"),
        F.count("next_stock_qty").over(w_hist).alias("category_count_next_stock_qty"),
        F.rank().over(w6).alias("cumulative_stock_qty_rank"),
        F.percent_rank().over(w7).alias("stock_qty_percent_rank"),
        F.max("row_seq").over(w_hist).alias("category_max_row_seq"),
    )
)

kept_keys_2 = (
    with_windows_products
    .select("id", "supplier_id")
    .intersectAll(
        df_products
        .filter(F.col("brand") != "Stark")
        .select("id", "supplier_id")
    )
)

verified = (
    with_windows_products
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "cumulative_stock_qty",
        "row_seq",
        "category_stddev_samp_stock_qty",
        "category_count_next_stock_qty",
    )
    .orderBy(F.col("category_stddev_samp_stock_qty").asc_nulls_last())
)
