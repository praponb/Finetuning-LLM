from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("st"), F.col("o.store_id") == F.col("st.id"), "left")
    .filter((F.lower(F.trim(F.col("st.store_name"))) == "riverside") & (F.col("o.shipping_fee") <= 34))
    .select(F.col("s.id"), F.col("s.carrier"), F.col("o.amount"), F.col("st.region"))
    .orderBy(F.col("amount").desc())
)
