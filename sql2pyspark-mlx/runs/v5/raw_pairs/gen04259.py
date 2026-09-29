from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 3]))
    .groupBy("store_id")
    .agg(
        F.count("*").alias("row_count"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .filter(F.col("st.row_count") > 6)
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("st.row_count"),
        F.col("st.units_range"),
    )
)
