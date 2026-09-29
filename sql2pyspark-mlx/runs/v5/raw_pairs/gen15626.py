from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.month("last_restock").isin([4, 6, 10]))
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 6))
)

summary = (
    base
    .groupBy(
        F.when(F.col("quantity") >= 236, "high").when(F.col("quantity") >= 95, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    summary
    .filter(F.col("stddev_quantity") >= 30)
    .orderBy(F.col("avg_quantity").desc(), "stddev_quantity")
)
