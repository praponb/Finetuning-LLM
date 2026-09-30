from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("cost") < 680)
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("cost").alias("unique_brands"),
        F.min("price").alias("min_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .filter(F.col("pbs.unique_brands") >= 9)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("pbs.unique_brands"),
        F.col("pbs.min_price"),
    )
    .orderBy(F.col("id").desc())
)
