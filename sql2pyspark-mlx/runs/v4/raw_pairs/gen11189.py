from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.month("last_restock").isin([7, 9, 12]))
    .withColumn("last_restock_period", F.date_trunc("MONTH", "last_restock"))
)

last_restock_period_cohort_summary = (
    valid_inventory
    .groupBy(
        F.when(F.year("last_restock_period") < 2024, "before_2024").when(F.year("last_restock_period") == 2024, "2024").otherwise("after_2024").alias("last_restock_period_cohort"),
    )
    .agg(F.min("quantity").alias("min_quantity"))
)

result = last_restock_period_cohort_summary
