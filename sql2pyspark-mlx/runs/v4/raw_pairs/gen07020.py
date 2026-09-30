from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .filter(F.col("products.brand").isin(["Stark", "Globex"]) & F.col("products.product_name").isNotNull())
    .select(
        F.col("suppliers.supplier_name"),
        F.col("suppliers.rating"),
        F.col("suppliers.city"),
        F.col("products.supplier_id"),
        F.col("products.category"),
        F.col("products.cost"),
    )
    .orderBy(F.col("city").desc(), "category")
)
