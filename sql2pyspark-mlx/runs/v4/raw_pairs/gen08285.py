from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("store_id").isin([2, 3, 4]))

last_restock_cohort_summary = (
    filtered_inventory
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        F.when(F.col("quantity") >= 233, "high").when(F.col("quantity") >= 58, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    last_restock_cohort_summary
    .select("last_restock_cohort", "quantity_bucket", "min_quantity")
)
