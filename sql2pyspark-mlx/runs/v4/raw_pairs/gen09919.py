from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.opened_date") > "2024-12-26") | (F.col("ss.sale_date") < "2023-03-01"))
    .select(
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("ss.store_id"),
    )
    .orderBy(F.col("store_id").desc())
)
