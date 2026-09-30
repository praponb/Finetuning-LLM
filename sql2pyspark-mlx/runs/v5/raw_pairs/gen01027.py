from pyspark.sql import functions as F

valid_inventory = df_inventory.filter(F.col("last_restock") < "2024-08-01")

last_restock_cohort_summary = (
    valid_inventory
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        "store_id",
    )
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    last_restock_cohort_summary
    .select("last_restock_cohort", "store_id", "max_quantity")
    .orderBy(F.col("last_restock_cohort").desc(), "store_id")
    .limit(3)
)
