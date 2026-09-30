from pyspark.sql import functions as F

kept = (
    df_products
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isNull()), "left_semi")
)

result = (
    kept
    .rollup("category")
    .agg(
        F.sum(F.when(F.col("brand") == "Acme", F.col("price"))).alias("acme_total_price"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
        F.stddev_samp("stock_qty").alias("stock_qty_stddev"),
        F.sum(F.when(F.col("category") == "Books", F.col("stock_qty"))).alias("books_total_stock_qty"),
    )
)
