from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("ss.revenue") > 3700)
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("p.supplier_id"),
        F.round(F.col("ss.units"), 1).alias("units_rounded"),
    )
    .orderBy("opened_date", "sale_date")
)
