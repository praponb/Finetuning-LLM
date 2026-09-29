from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 760, "LARGE").otherwise("SMALL").alias("cost_tier"),
        "category",
    )
    .agg(
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("brand") == "Initech", 1)), F.lit(0)), 2).alias("stock_qty_per_initech"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
)
