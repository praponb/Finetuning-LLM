from pyspark.sql import functions as F

store_totals = (
    df_orders
    .groupBy("store_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("st.status_list"),
    )
    .orderBy(F.col("id").desc())
)
