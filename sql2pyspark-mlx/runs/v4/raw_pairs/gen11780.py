from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "inner")
    .select(
        F.col("s.cost"),
        F.col("o.customer_id"),
        F.col("st.store_name"),
        F.col("st.opened_date"),
        F.add_months(F.col("s.delivered_date"), 6).alias("delivered_date_plus_months"),
    )
)
