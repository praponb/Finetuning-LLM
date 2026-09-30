from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("units").between(19, 30)).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("s.region"), F.col("s.sqft"))
)
