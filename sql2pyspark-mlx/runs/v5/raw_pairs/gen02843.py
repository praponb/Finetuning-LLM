from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .select(
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
        F.col("products.cost"),
        F.col("products.price"),
        F.col("products.category"),
    )
)
