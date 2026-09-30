from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(~(~F.col("s.region").isin(["West", "North", "East"]) | (F.col("s.opened_date") <= "2023-11-04")))
    .select(
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.round((F.col("ss.revenue") / F.col("ss.units")), 2).alias("revenue_per_unit"),
    )
    .orderBy(F.col("revenue").desc())
)
