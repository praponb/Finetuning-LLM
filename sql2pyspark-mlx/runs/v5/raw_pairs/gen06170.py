from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter((F.length(F.col("p.product_name")) < 10) & (F.col("p.category") == "Fashion"))
    .select(
        F.col("ss.product_id"),
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.datediff(F.current_date(), F.col("ss.sale_date")).alias("days_since_sale_date"),
    )
)
