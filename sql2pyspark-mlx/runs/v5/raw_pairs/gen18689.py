from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.opened_date") >= "2023-11-01")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.add_months(F.col("ss.sale_date"), 6).alias("sale_date_plus_months"),
    )
    .orderBy("store_name", F.col("product_id").desc())
)
