from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("s.store_name").like("%Town%") & (F.col("ss.units") > 18))
    .select(
        F.col("ss.revenue"),
        F.col("s.opened_date"),
        F.col("p.cost"),
        F.col("p.launch_date"),
    )
    .orderBy(F.col("revenue").desc())
)
