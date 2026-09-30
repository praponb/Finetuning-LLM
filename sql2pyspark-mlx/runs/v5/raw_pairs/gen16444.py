from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.filter(F.col("stock_qty") != 185).alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left_semi")
    .filter(F.col("suppliers.rating").between(2.3, 4.1))
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.rating"),
    )
    .orderBy("id", F.col("rating").desc())
    .limit(1)
)
