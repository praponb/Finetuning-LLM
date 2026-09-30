from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.stddev_quantity") >= 21)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("st.quantity_range"),
        F.col("st.stddev_quantity"),
    )
)
