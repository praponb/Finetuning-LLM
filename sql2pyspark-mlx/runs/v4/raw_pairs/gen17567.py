from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .filter(F.col("s.carrier") != "Flash")
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.delivered_date"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("st.opened_date"),
        F.col("st.store_name"),
    )
    .orderBy(F.col("customer_id").desc(), F.col("discount").desc())
)
