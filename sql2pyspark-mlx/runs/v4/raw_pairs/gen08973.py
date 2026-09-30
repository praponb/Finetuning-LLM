from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "outer")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.brand").isin(["Umbrella", "Initech"]) & (F.col("p.stock_qty") <= 119))
    .select(
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("p.cost"),
    )
)
