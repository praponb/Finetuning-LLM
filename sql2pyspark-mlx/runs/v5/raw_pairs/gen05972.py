from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3, 5, 9]) & (F.col("quantity") > 166))
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        "product_id",
    )
    .agg(F.max("quantity").alias("max_quantity"))
    .orderBy("product_id")
)
