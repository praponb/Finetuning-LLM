from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.avg_revenue") > 1500)
    .select(F.col("s.region"), F.col("st.avg_revenue"))
    .orderBy(F.col("avg_revenue").desc())
)
