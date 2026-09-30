from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.units") != 12)
    .groupBy(F.col("s.city"))
    .agg(
        F.countDistinct(F.col("ss.revenue")).alias("unique_stores"),
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
    )
    .orderBy(F.col("total_revenue").desc(), F.col("unique_stores").desc())
)
