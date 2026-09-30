from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("quantity").between(163, 179))
    .groupBy("store_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .select(F.col("s.id"), F.col("s.store_name"), F.col("st.avg_quantity"))
    .orderBy(F.col("avg_quantity").desc())
)
