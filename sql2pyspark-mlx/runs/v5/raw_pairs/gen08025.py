from pyspark.sql import functions as F

enriched = (
    df_order_items
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.stock_qty"))
)

result = (
    enriched
    .groupBy("product_id")
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
    .orderBy(F.col("distinct_order_ids").desc_nulls_last())
)
