from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("quantity") <= 184)
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.quantity_count") >= 4)
    .select(
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("st.stddev_quantity"),
        F.col("st.quantity_count"),
    )
    .orderBy("store_name", F.col("quantity_count").desc())
)
