from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("st.quantity_count"),
        F.col("st.max_quantity"),
    )
)
