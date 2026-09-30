from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft").between(8593, 12820))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("s.city"),
        F.col("s.region"),
    )
    .orderBy("city")
)
