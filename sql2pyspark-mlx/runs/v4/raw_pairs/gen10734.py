from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.min("sale_date").alias("first_sale_date"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("st"), F.col("s.id") == F.col("st.store_id"), "inner")
    .select(F.col("s.sqft"), F.col("st.first_sale_date"))
    .orderBy(F.col("sqft").desc(), "first_sale_date")
)
