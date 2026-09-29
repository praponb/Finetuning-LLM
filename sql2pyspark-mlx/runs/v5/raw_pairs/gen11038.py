from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.min(F.col("ss.units")).alias("min_units"),
        F.min(F.col("ss.revenue")).alias("min_revenue"),
    )
    .filter(F.col("min_units") > 11)
    .orderBy("min_units", "id", "city")
    .limit(50)
)
