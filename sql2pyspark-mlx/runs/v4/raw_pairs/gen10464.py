from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2023-03-02", "2025-05-25") & (F.col("category") != "Electronics") & (F.year("launch_date") == 2024))
    .groupBy(
        F.month("launch_date").alias("launch_month"),
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
        F.when(F.col("price") >= 990, "high").when(F.col("price") >= 440, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.min("price").alias("min_price"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .orderBy(F.col("min_price").desc(), F.col("launch_month").desc())
)
