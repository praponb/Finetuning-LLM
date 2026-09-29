from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("product_name").like("%Bac%") & (F.array_contains("tags", "sale") | (F.col("price") <= 370)))
    .cube(F.quarter("launch_date").alias("launch_quarter"), "category")
    .agg(
        F.max("price").alias("max_price"),
        F.sum(F.when(F.col("category") == "Grocery", F.col("stock_qty")).otherwise(0)).alias("grocery_stock_qty"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .orderBy("launch_quarter")
)
