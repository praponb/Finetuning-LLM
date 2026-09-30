from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
    )
    .filter(F.col("quantity_count") >= 1)
    .orderBy(F.col("quantity_count").desc(), "opened_month")
    .limit(5)
)
