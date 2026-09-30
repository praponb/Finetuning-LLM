from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .groupBy("store_id")
    .agg(F.max("order_date").alias("last_order_date"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("st.last_order_date"),
    )
    .orderBy("opened_date")
)
