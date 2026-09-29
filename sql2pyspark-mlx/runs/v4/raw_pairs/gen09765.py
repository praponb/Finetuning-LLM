from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .filter(F.col("products.launch_date") <= "2024-10-01")
    .select(
        F.col("products.supplier_id"),
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
        (F.col("products.stock_qty") - F.col("products.cost")).alias("stock_qty_minus_cost"),
    )
)
