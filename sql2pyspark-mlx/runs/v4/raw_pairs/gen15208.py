from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "left")
    .select(
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.coalesce(F.col("s.sqft"), F.lit(0)).alias("sqft_filled"),
    )
)
