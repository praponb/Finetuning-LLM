from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.store_id").isin([2, 4, 5, 6]))
    .select(
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
    )
    .orderBy(F.col("sale_date").desc())
)
