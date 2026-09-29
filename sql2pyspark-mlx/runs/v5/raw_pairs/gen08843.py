from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.store_name"), F.col("st.unique_products"))
    .orderBy("store_name")
)
