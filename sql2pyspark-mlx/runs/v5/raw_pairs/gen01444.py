from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("product_id") == 4)
    .select(
        "id",
        "store_id",
        "product_id",
        F.last_day("sale_date").alias("sale_date_month_end"),
        F.floor("units").alias("units_whole"),
    )
)

result = (
    cleaned
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.region").alias("region"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("a.units_whole")).alias("total_units_whole"),
    )
    .orderBy(F.col("total_units_whole").asc_nulls_last())
)
