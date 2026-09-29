from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") >= "2024-06-01") & (F.col("category") == "Sports"))
    .groupBy(
        F.when(F.col("category").isin(["Books", "Sports"]), "priority").otherwise("other").alias("category_group"),
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.count("cost").alias("cost_count"),
        F.countDistinct("stock_qty").alias("unique_categorys"),
    )
    .orderBy("cost_count")
)
