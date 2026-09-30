from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([6, 9]) & F.col("quantity").between(129, 163))
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.count("*").alias("inventory_count"),
    )
)
