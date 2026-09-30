from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("quantity") <= 60)
    .groupBy("store_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(F.col("s.opened_date"), F.col("s.city"), F.col("st.unique_products"))
    .orderBy(F.col("city").desc())
)
