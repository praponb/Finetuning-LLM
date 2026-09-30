from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("category") == "Home", F.col("stock_qty")).otherwise(0)).alias("home_stock_qty"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
    .orderBy("max_stock_qty")
)
