from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 5]))
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_count"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("st.total_count"),
        F.col("st.revenue_range"),
    )
)
