from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("st.quantity_count"),
    )
    .orderBy("opened_date")
)
