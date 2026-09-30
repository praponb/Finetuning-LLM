from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("stock_qty") >= 321)
    .groupBy("supplier_id")
    .agg(F.countDistinct("stock_qty").alias("unique_categorys"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .filter(F.col("pbs.unique_categorys") >= 9)
    .select(F.col("s.supplier_name"), F.col("s.city"), F.col("pbs.unique_categorys"))
    .orderBy("unique_categorys")
)
