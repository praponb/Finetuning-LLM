from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") > "2025-06-25")
    .groupBy(
        "category",
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.max("launch_date").alias("latest_launch_date"),
    )
)
