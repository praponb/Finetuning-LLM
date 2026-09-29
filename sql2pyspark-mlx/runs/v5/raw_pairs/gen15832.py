from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull() & ((F.col("brand") == "Acme") | ~F.col("product_name").like("%Mat%")))
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(
        F.max("stock_qty").alias("max_stock_qty"),
        F.count("cost").alias("cost_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
    )
    .filter(F.col("max_stock_qty") >= 190)
)
