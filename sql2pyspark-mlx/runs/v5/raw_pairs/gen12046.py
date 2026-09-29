from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter(F.col("launch_date") <= "2023-10-01")
    .withColumn("least_cost_stock_qty", F.least("cost", "stock_qty"))
)

products_stats = (
    valid_products
    .groupBy("category")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.avg("least_cost_stock_qty").alias("avg_least_cost_stock_qty"),
    )
)

result = (
    products_stats
    .select("category", "category_list", "avg_least_cost_stock_qty")
    .orderBy(F.col("avg_least_cost_stock_qty").desc(), "category")
)
