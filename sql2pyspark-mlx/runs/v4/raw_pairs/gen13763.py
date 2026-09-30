from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "clearance"))
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
        F.when(F.col("cost") >= 800, "high").when(F.col("cost") >= 170, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.max("stock_qty").alias("max_stock_qty"),
        F.avg("cost").alias("avg_cost"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
    .filter(F.col("total_stock_qty") > 1200)
    .drop("total_stock_qty")
)
