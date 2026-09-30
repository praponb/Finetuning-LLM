from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg(F.avg(F.col("o.amount")).alias("avg_amount"))
)
