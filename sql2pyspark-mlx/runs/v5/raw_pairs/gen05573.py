from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.min_quantity") >= 100)
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("st.unique_stores"),
        F.col("st.min_quantity"),
    )
)
