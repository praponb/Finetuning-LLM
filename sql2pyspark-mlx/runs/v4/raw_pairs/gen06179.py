from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("category") == "Grocery")
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("stock_qty").alias("unique_suppliers"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.max_stock_qty") > 290)
    .select(
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("t.unique_suppliers"),
        F.col("t.max_stock_qty"),
    )
    .orderBy(F.col("max_stock_qty").desc(), F.col("unique_suppliers").desc())
)
