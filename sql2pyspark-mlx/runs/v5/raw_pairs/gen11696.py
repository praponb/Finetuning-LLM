from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") > "2024-06-01") & (F.col("category") == "Home"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.min("launch_date").alias("min_launch_date"),
    )
)
