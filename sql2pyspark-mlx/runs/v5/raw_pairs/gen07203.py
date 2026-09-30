from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") > "2025-05-01")
    .groupBy("category")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
    )
    .orderBy(F.col("category").desc(), "pct_home")
)
