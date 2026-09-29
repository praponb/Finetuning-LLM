from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") < "2023-06-01") & F.col("product_id").isin([4, 6, 7, 9]) & F.col("store_id").isin([3, 4, 5]))
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") >= 2)
    .drop("quantity_count")
)
