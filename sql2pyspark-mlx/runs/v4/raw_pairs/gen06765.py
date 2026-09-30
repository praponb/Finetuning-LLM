from pyspark.sql import functions as F

result = (
    df_products
    .cube(
        F.when(F.col("stock_qty") >= 408, "HIGH").otherwise("LOW").alias("stock_qty_tier"),
    )
    .agg(
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost_2"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("category") == "Grocery", 1)), F.lit(0)), 2).alias("stock_qty_per_grocery"),
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
    )
)
