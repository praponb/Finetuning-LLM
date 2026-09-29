from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter(F.col("st.store_name").like("Mal%") & ~F.col("st.region").isin(["East", "West"]))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.delivered_date"),
        F.col("o.store_id"),
        F.col("st.sqft"),
        F.col("st.store_name"),
    )
)
