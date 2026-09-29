from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & ((F.year("launch_date") >= 2023) | (F.col("cost") > 180)))
)

category_group_summary = (
    recent_products
    .groupBy(
        F.when(F.col("category").isin(["Books", "Home"]), "tier_1").otherwise("other").alias("category_group"),
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(F.avg("stock_qty").alias("avg_stock_qty"))
)

result = category_group_summary.orderBy(F.col("category_group").desc())
