from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(F.col("discount") < 0.2)
    .groupBy("store_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.cnt") > 9)
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.opened_date"), F.col("ss.cnt"))
)
