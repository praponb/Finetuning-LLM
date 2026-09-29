from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .filter(F.col("products.product_name").like("L%"))
    .groupBy(F.col("suppliers.id"), F.col("suppliers.city"))
    .agg(
        F.count(F.col("products.id")).alias("product_count"),
        F.countDistinct(F.col("products.stock_qty")).alias("unique_suppliers"),
        (F.max(F.col("products.price")) - F.min(F.col("products.price"))).alias("price_range"),
    )
)
