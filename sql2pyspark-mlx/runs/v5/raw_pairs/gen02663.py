from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grocery"),
    )
)
