from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.customer_id").isNull())
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"), F.col("s.region"))
    .agg(F.avg(F.col("o.amount")).alias("avg_amount"))
    .orderBy(F.col("region").desc())
)
