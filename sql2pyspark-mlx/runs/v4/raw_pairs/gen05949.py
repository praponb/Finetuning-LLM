from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "outer")
    .filter(F.col("s.store_name").isNotNull())
    .groupBy(
        F.col("s.city"),
        F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(F.round(F.stddev(F.col("o.amount")), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") >= 38)
    .orderBy("opened_month", "stddev_amount")
)
