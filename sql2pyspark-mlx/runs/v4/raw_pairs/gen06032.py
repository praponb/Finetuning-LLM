from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        "store_id",
    )
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.max("quantity").alias("max_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)
