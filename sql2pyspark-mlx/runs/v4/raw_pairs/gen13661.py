from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.year("launch_date") >= 2024)
    .groupBy("supplier_id")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.city"),
        F.col("t.total_stock_qty"),
        F.col("t.brand_list"),
    )
    .orderBy(F.col("country").desc(), F.col("city").desc(), "id")
    .limit(1)
)
