from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("st.id") == F.col("o.store_id"), "left")
    .groupBy(
        F.year(F.col("st.opened_date")).alias("opened_year"),
        F.month(F.col("st.opened_date")).alias("opened_month"),
    )
    .agg(F.sum(F.when(F.col("s.status") == "LOST", 1).otherwise(0)).alias("lost_count"))
)
