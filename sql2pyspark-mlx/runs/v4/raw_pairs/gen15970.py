from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .groupBy(F.col("suppliers.id"), F.col("suppliers.city"))
    .agg(
        F.count(F.col("products.id")).alias("product_count"),
        F.sum(F.when(F.col("products.brand") == "Globex", F.col("products.stock_qty")).otherwise(0)).alias("globex_stock_qty"),
        F.countDistinct(F.col("products.price")).alias("unique_suppliers"),
    )
    .filter(F.col("unique_suppliers") >= 5)
    .orderBy(F.col("unique_suppliers").desc())
)
