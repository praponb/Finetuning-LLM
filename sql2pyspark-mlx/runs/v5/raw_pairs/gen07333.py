from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("*").alias("num_inventory"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.num_inventory") > 8)
    .select(F.col("s.sqft"), F.col("st.num_inventory"), F.col("st.unique_stores"))
)
