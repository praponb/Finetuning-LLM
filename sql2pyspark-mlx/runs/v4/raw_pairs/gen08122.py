from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").isNotNull() | (F.col("quantity") > 219))
    .groupBy(
        F.when(F.col("quantity") >= 240, "high").when(F.col("quantity") >= 65, "mid").otherwise("low").alias("quantity_bucket"),
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
    .orderBy("last_restock_month")
)
