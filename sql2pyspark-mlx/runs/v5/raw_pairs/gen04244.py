from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.avg_revenue") > 74)
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("st.avg_units"),
        F.col("st.avg_revenue"),
    )
    .orderBy(F.col("store_name").desc(), "id")
    .limit(10)
)
