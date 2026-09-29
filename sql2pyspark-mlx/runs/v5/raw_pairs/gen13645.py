from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.date_trunc("WEEK", "last_restock").alias("week_start"),
        F.date_trunc("MONTH", "last_restock").alias("activity_month"),
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") > 0)
    .orderBy(F.col("activity_month").desc_nulls_last())
)
