from pyspark.sql import functions as F

kept = (
    df_products
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.unit_price") < 370), "left_anti")
)

result = (
    kept
    .cube("supplier_id")
    .agg(
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_rows"),
        F.percentile_approx("stock_qty", 0.5).alias("stock_qty_p50"),
        F.countDistinct("category").alias("distinct_categories"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.corr("stock_qty", "cost").alias("corr_stock_qty_cost"),
        F.grouping_id().alias("grouping_id"),
    )
)
