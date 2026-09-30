from pyspark.sql import functions as F

with_dims = (
    df_products
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.rating"), F.col("p2.supplier_name"))
)

result = (
    with_dims
    .withColumn("stock_qty_tier", F.when(F.col("stock_qty") >= 372, "LARGE").otherwise("SMALL"))
    .rollup("stock_qty_tier")
    .agg(
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_rows"),
        F.max(F.when(F.col("brand") == "Initech", F.col("price"))).alias("initech_max_price"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.countDistinct("category").alias("distinct_categories"),
        F.round(F.avg("price"), 2).alias("avg_price_2"),
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("distinct_categories") < 1)
)
