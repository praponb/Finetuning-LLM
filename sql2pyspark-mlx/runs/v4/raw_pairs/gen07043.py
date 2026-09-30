from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_trunc("month", "last_restock").alias("activity_month"))
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
)
