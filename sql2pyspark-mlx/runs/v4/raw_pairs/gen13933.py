from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.size("tags") > 1)
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Acme", F.col("cost")).otherwise(0)).alias("acme_cost"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("price")).otherwise(0)).alias("acme_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("t.acme_cost"),
        F.col("t.acme_price"),
    )
    .orderBy("supplier_name")
)
