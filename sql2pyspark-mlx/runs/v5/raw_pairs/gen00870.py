from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.min("sale_date").alias("first_sale_date"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(F.col("s.opened_date"), F.col("s.sqft"), F.col("t.first_sale_date"))
    .orderBy("first_sale_date", "sqft")
)
