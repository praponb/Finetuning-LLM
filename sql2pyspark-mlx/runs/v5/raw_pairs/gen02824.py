from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([20, 36]) & F.col("supplier_id").isNotNull() & (F.col("category") == "Sports"))
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grocery"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .orderBy(F.col("pct_grocery").desc())
)
