from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.countDistinct("stock_qty").alias("unique_categorys"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.unique_categorys") > 6)
    .select(
        F.col("s.supplier_name"),
        F.col("t.stddev_price"),
        F.col("t.unique_categorys"),
    )
    .orderBy("stddev_price")
)
