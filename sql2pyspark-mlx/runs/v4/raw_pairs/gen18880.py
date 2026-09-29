from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(F.col("shipping_fee").between(14, 29))
    .groupBy("store_id")
    .agg(F.min("discount").alias("min_discount"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "left")
    .select(F.col("s.store_name"), F.col("st.min_discount"))
    .orderBy("min_discount")
)
