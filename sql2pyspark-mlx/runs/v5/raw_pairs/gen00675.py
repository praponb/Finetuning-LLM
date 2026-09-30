from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.city").isin(["Singapore", "Berlin", "Chiang Mai"]))
    .select(
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("ss.revenue"),
        F.col("s.region"),
        F.col("s.sqft"),
    )
    .orderBy(F.col("units").desc())
)
