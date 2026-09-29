from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 730, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
        "category",
    )
    .agg(
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("price")).otherwise(0)).alias("electronics_price"),
        F.size(F.collect_set("category")).alias("num_categories"),
    )
    .orderBy(F.col("electronics_price").desc_nulls_first())
)
