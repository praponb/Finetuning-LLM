from pyspark.sql import functions as F

store_totals = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.max("order_date").alias("last_order_date"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .filter(F.col("st.total_amount") >= 5600)
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("st.total_amount"),
        F.col("st.last_order_date"),
    )
    .orderBy(F.col("region").desc(), "store_name")
)
