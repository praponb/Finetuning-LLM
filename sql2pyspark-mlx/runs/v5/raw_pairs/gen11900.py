from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.col("quantity").isNotNull() & F.col("product_id").isin([3, 4, 6]))
    .withColumn("last_restock_period", F.date_trunc("month", "last_restock"))
)

last_restock_period_month_summary = (
    valid_inventory
    .groupBy(
        F.date_format("last_restock_period", "yyyy-MM").alias("last_restock_period_month"),
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    last_restock_period_month_summary
    .filter(F.col("stddev_quantity") > 62)
    .select("last_restock_period_month", "stddev_quantity")
    .orderBy(F.col("last_restock_period_month").desc(), F.col("stddev_quantity").desc())
    .limit(10)
)
