from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("stock_qty") < 271)
    .groupBy("supplier_id")
    .agg(
        F.sum("price").alias("total_price"),
        F.min("launch_date").alias("first_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(F.col("s.city"), F.col("t.total_price"), F.col("t.first_launch_date"))
    .orderBy(F.col("total_price").desc())
)
