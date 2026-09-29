from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.total_count") > 7)
    .select(
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("st.max_quantity"),
        F.col("st.total_count"),
    )
)
