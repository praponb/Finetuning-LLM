from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.product_id").isin([1, 2]))
    .groupBy(
        F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"),
        F.year(F.col("s.opened_date")).alias("opened_year"),
    )
    .agg(
        F.count(F.col("ss.units")).alias("units_count"),
        F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"),
    )
    .orderBy(F.col("opened_year").desc())
)
