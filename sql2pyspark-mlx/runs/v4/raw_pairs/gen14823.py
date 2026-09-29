from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .filter(F.col("revenue") <= 3200)
    .groupBy("store_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.min("sale_date").alias("min_sale_date"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("st.first_sale_date"),
        F.col("st.min_sale_date"),
    )
    .orderBy(F.col("id").desc(), "min_sale_date")
)
