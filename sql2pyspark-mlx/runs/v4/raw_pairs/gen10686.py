from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.col("s.order_id").isNotNull())
    .select(
        F.col("s.cost"),
        F.col("s.carrier"),
        F.col("o.status"),
        F.col("o.amount"),
        F.col("st.city"),
        F.col("st.opened_date"),
    )
    .orderBy("opened_date")
)
