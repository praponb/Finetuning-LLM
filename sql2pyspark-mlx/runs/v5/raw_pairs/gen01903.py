from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy(F.col("price").asc_nulls_first())
w2 = Window.partitionBy("category", "supplier_id").orderBy("stock_qty", "id")
w_hist = Window.partitionBy("category")

base = (
    df_products
    .filter(F.col("stock_qty").between(134, 349))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "stock_qty",
        "supplier_id",
        F.upper(F.trim("product_name")).alias("product_name_normalized"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity") < 5), "left_anti")
)

metrics = (
    eligible
    .select(
        "id",
        "product_name",
        "category",
        "stock_qty",
        F.rank().over(w).alias("price_rank"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

eligible_products = (
    metrics
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.store_id").isin([1, 3, 4])), "left_anti")
)

scored = (
    eligible_products
    .select(
        "id",
        "product_name",
        "category",
        F.max("row_seq").over(w_hist).alias("category_max_row_seq"),
        F.stddev_samp("stock_qty").over(w_hist).alias("category_stddev_samp_stock_qty"),
    )
)

result = (
    scored
    .cube("category")
    .agg(
        F.max(F.when(F.col("category") == "Fashion", F.col("category_max_row_seq"))).alias("fashion_max_category_max_row_seq"),
        F.round(F.sum("category_max_row_seq") / F.nullif(F.count(F.when(F.col("category") == "Sports", 1)), F.lit(0)), 2).alias("category_max_row_seq_per_sports"),
        F.percentile_approx("category_stddev_samp_stock_qty", [0.25, 0.5, 0.75]).alias("category_stddev_samp_stock_qty_quartiles"),
    )
    .filter(F.col("fashion_max_category_max_row_seq") >= 0)
    .orderBy(F.col("category_max_row_seq_per_sports").asc_nulls_last())
)
