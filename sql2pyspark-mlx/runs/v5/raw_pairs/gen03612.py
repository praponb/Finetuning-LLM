from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("stock_qty").between(104, 274))
    .groupBy("supplier_id")
    .agg(F.max("launch_date").alias("latest_launch_date"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("P"), F.col("s.id") == F.col("P.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.country"), F.col("P.latest_launch_date"))
    .orderBy("latest_launch_date", "country")
)
