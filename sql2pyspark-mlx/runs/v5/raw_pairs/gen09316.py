from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.col("product_id").isin([2, 4, 8]) & (F.col("product_id") == 2))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

agg = (
    valid_inventory
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.count("*").alias("inventory_count"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)

result = agg.select("last_restock_cohort", "inventory_count")
