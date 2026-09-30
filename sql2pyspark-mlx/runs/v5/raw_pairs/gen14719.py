from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5]) & F.col("product_id").isin([5, 6, 9]))
)

last_restock_cohort_summary = (
    recent_inventory
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(F.min("last_restock").alias("first_last_restock"))
)

result = (
    last_restock_cohort_summary
    .select("last_restock_cohort", "first_last_restock")
    .orderBy(F.col("first_last_restock").desc())
)
