from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.year("last_restock") >= 2025)
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        "product_id",
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") >= 90)
    .drop("max_quantity")
)
