from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.filter(F.col("store_id").isin([2, 4])).alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left_semi")
    .filter(~F.col("s.city").isin(["London", "Bangkok"]))
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("s.city"))
    .orderBy("city")
)
