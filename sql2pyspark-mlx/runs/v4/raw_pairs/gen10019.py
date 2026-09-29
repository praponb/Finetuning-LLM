from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("s.city"),
        (F.col("ss.units") * F.col("ss.revenue")).alias("revenue_per_unit"),
    )
    .orderBy("opened_date", "id")
    .limit(5)
)
