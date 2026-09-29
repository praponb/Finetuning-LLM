from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(F.avg("quantity").alias("avg_quantity"))
    .orderBy("last_restock_cohort")
)
