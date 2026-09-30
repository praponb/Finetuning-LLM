from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(F.sort_array(F.collect_set("category")).alias("category_list"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("pbs"), F.col("s.id") == F.col("pbs.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("pbs.category_list"))
    .orderBy(F.col("id").desc())
)
