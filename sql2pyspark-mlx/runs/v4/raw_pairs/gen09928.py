from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.opened_date") >= "2023-08-29")
    .groupBy(
        F.col("s.region"),
        F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(F.min(F.col("o.amount")).alias("min_amount"))
)
