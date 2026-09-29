from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("products_by_supplier"), F.col("suppliers.id") == F.col("products_by_supplier.supplier_id"), "inner")
    .filter(F.col("products_by_supplier.stddev_cost") >= 60)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("products_by_supplier.stddev_cost"),
    )
    .orderBy("stddev_cost", F.col("supplier_name").desc())
)
