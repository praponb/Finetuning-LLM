from pyspark.sql import functions as F

matched_keys = (
    df_products
    .select("supplier_id", "brand")
    .subtract(
        df_products
        .filter(F.col("supplier_id").isin([6, 31]))
        .select("supplier_id", "brand")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("YEAR", "launch_date").alias("year_start"))
    .agg(
        F.max(F.when(F.col("category") == "Fashion", F.col("stock_qty"))).alias("fashion_max_stock_qty"),
        F.stddev_samp("price").alias("price_stddev"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
        F.corr("stock_qty", "price").alias("corr_stock_qty_price"),
    )
    .orderBy("corr_stock_qty_price")
)
