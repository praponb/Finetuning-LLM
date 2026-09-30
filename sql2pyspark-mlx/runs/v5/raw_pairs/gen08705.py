from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.supplier_id") == 17)
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("p.product_name"),
        F.col("p.brand"),
    )
    .orderBy("opened_date")
)
