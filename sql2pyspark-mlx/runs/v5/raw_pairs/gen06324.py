from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
    .filter(F.col("avg_quantity") >= 120)
    .orderBy(F.col("unique_stores").desc())
)
