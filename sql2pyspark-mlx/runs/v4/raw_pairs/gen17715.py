from pyspark.sql import functions as F

not_flagged = (
    df_products
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock") < F.col("s.launch_date")), "left_anti")
)

result = (
    not_flagged
    .cube("brand")
    .agg(
        F.countDistinct("category").alias("distinct_categories"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
)
