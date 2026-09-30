from pyspark.sql import functions as F

filtered_products = df_products.filter(F.col("stock_qty") != 242)

products_stats = (
    filtered_products
    .groupBy("category")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Electronics", 1).otherwise(0))) / F.count("*")), 1).alias("pct_electronics"),
    )
)

result = (
    products_stats
    .orderBy("category")
    .limit(20)
)
