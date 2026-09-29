from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "clearance"))
    .select(
        F.col("s.city"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.col("p.brand"),
        F.initcap(F.trim(F.col("s.city"))).alias("city_title"),
    )
    .orderBy(F.col("city").desc())
)
