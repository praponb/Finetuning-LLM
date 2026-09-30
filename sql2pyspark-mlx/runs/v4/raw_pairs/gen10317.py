from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id").isin([2, 3, 5, 10]) & (F.year("last_restock") == 2025)) | F.col("product_id").isin([4, 7]))
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)
