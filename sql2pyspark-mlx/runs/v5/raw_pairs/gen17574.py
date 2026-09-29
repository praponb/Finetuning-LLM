from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter(F.col("products.cost").between(490, 610))
    .select(
        F.col("products.id"),
        F.col("products.cost"),
        F.col("products.product_name"),
        F.col("products.supplier_id"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.city"),
    )
    .orderBy("cost", "id")
    .limit(1)
)
