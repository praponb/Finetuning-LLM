from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter(F.col("o.customer_id").isNotNull())
    .groupBy(F.col("o.status"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.carrier") == "DHL", 1).otherwise(0))) / F.count("*")), 1).alias("pct_dhl"),
    )
)
