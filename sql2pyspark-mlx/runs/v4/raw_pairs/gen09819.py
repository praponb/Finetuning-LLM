from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.countDistinct("price").alias("unique_categorys"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("t.last_launch_date"),
        F.col("t.unique_categorys"),
    )
)
