from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.col("supplier_id").isin([3, 5, 29]))
    .withColumn("launch_date_plus_months", F.add_months("launch_date", 3))
)

summary = (
    recent_products
    .groupBy(
        F.when(F.col("cost") >= 620, "high").when(F.col("cost") >= 210, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
)

result = (
    summary
    .select("cost_bucket", "min_cost", "category_list")
    .orderBy("cost_bucket", F.col("min_cost").desc())
)
