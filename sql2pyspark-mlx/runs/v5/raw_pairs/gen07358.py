from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .filter(F.col("units") >= 35)
    .groupBy("store_id")
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(F.col("s.city"), F.col("st.stddev_revenue"))
    .orderBy(F.col("stddev_revenue").desc())
)
