from pyspark.sql import functions as F

not_flagged = (
    df_products
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id") == 23), "left_semi")
)

result = (
    not_flagged
    .groupBy(F.when(F.col("cost") >= 550, "HIGH").otherwise("LOW").alias("cost_tier"))
    .agg(
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_rows"),
        F.countDistinct("category").alias("distinct_categories"),
        F.sum(F.when(F.col("category") == "Fashion", F.col("cost"))).alias("fashion_total_cost"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cost_tier").desc_nulls_last())
)
