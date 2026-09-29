from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter((F.col("ss.units") != 21) & (F.col("p.stock_qty") < 322))
    .select(
        F.col("ss.sale_date"),
        F.col("p.product_name"),
        F.col("s.city"),
        F.col("s.country"),
    )
)
