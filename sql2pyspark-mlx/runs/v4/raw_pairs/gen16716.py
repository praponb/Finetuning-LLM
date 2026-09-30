from pyspark.sql import functions as F

excluded_suppliers = df_suppliers.filter(F.col("supplier_name").like("%Lion%")).select("id")

result = (
    df_products.alias("products")
    .join(excluded_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left_anti")
    .filter(F.col("products.supplier_id").isNotNull() | F.lit(excluded_suppliers.isEmpty()))
    .select(
        F.col("products.id"),
        F.col("products.stock_qty"),
        F.col("products.product_name"),
        F.col("products.category"),
    )
)
