from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") > 330) & (F.col("category") == "Sports"))
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.count("*").alias("num_products"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.sum("cost").alias("total_cost"),
    )
)
