from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(F.col("shipping_fee") < 16)
    .groupBy("store_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_completed"),
        F.count("*").alias("order_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.order_count") > 6)
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("st.pct_completed"),
        F.col("st.order_count"),
    )
    .orderBy("order_count", F.col("id").desc())
)
